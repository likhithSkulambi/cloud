param(
    [string]$ProjectId = $env:GCP_PROJECT_ID,
    [string]$Region = $env:GCP_REGION,
    [string]$Dataset = $env:BIGQUERY_DATASET,
    [string]$BigQueryLocation = $env:BIGQUERY_LOCATION,
    [string]$Topic = $env:PUBSUB_TOPIC_ID,
    [string]$Subscription = "irrigation-alerts-sub",
    [string]$ServiceAccountName = "smart-irrigation-sa",
    [string]$Runtime = "python311",
    [string]$Memory = "512MB",
    [string]$Timeout = "540s",
    [int]$MinInstances = 0,
    [int]$MaxInstances = 5,
    [string]$WeatherCron = "0 2 * * *",
    [string]$EvaluateCron = "30 2 * * *",
    [string]$SchedulerTimezone = "UTC",
    [string]$SendGridApiKey = $env:SENDGRID_API_KEY,
    [string]$AlertFromEmail = $env:ALERT_FROM_EMAIL,
    [string]$AlertMinUrgency = $env:ALERT_MIN_URGENCY,
    [string]$WeatherLookbackDays = $env:WEATHER_LOOKBACK_DAYS,
    [string]$CorsOrigin = $env:CORS_ORIGIN,
    [string]$CloudRunServiceName = "smart-irrigation-dashboard",
    [string]$RepositoryName = "smart-irrigation",
    [int]$CloudRunMemory = 1024,
    [int]$CloudRunCpu = 1,
    [int]$CloudRunMaxInstances = 10
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Off
if (Test-Path variable:PSNativeCommandUseErrorActionPreference) {
    $global:PSNativeCommandUseErrorActionPreference = $false
}

# Use .cmd entry points to avoid PowerShell wrapper NativeCommandError noise.
Set-Alias -Name gcloud -Value gcloud.cmd -Scope Script
Set-Alias -Name bq -Value bq.cmd -Scope Script

function Write-Section {
    param([string]$Message)
    Write-Host ""
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    Write-Host "► $Message"
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
}

function Read-DotEnvFile {
    param([string]$Path)
    $values = @{}
    if (-not (Test-Path $Path)) {
        return $values
    }
    foreach ($line in Get-Content $Path) {
        $trimmed = $line.Trim()
        if (-not $trimmed -or $trimmed.StartsWith("#")) {
            continue
        }
        $parts = $trimmed -split "=", 2
        if ($parts.Count -eq 2) {
            $key = $parts[0].Trim()
            $value = $parts[1].Trim().Trim('"').Trim("'")
            $values[$key] = $value
        }
    }
    return $values
}

function Resolve-Setting {
    param(
        [string]$CurrentValue,
        [hashtable]$Settings,
        [string]$Key,
        [string]$DefaultValue = ""
    )
    if ($CurrentValue) {
        return $CurrentValue
    }
    if ($Settings.ContainsKey($Key) -and $Settings[$Key]) {
        return [string]$Settings[$Key]
    }
    return $DefaultValue
}

function Assert-CommandExists {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found. Install the Google Cloud SDK first."
    }
}

function Invoke-Checked {
    param(
        [scriptblock]$Command,
        [string]$FailureMessage
    )
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $output = & $Command 2>&1
    $ErrorActionPreference = $previousPreference
    if ($LASTEXITCODE -ne 0) {
        throw $FailureMessage
    }
    return $output
}

# Validate prerequisites
Assert-CommandExists -Name "gcloud"
Assert-CommandExists -Name "bq"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$configValues = Read-DotEnvFile -Path (Join-Path $repoRoot "config\.env")

$ProjectId = Resolve-Setting -CurrentValue $ProjectId -Settings $configValues -Key "GCP_PROJECT_ID"
$Region = Resolve-Setting -CurrentValue $Region -Settings $configValues -Key "GCP_REGION" -DefaultValue "us-central1"
$Dataset = Resolve-Setting -CurrentValue $Dataset -Settings $configValues -Key "BIGQUERY_DATASET" -DefaultValue "smart_irrigation"
$BigQueryLocation = Resolve-Setting -CurrentValue $BigQueryLocation -Settings $configValues -Key "BIGQUERY_LOCATION" -DefaultValue "US"
$Topic = Resolve-Setting -CurrentValue $Topic -Settings $configValues -Key "PUBSUB_TOPIC_ID" -DefaultValue "irrigation-alerts"
$SendGridApiKey = Resolve-Setting -CurrentValue $SendGridApiKey -Settings $configValues -Key "SENDGRID_API_KEY"
$AlertFromEmail = Resolve-Setting -CurrentValue $AlertFromEmail -Settings $configValues -Key "ALERT_FROM_EMAIL" -DefaultValue "alerts@example.com"
$AlertMinUrgency = Resolve-Setting -CurrentValue $AlertMinUrgency -Settings $configValues -Key "ALERT_MIN_URGENCY" -DefaultValue "HIGH"
$WeatherLookbackDays = Resolve-Setting -CurrentValue $WeatherLookbackDays -Settings $configValues -Key "WEATHER_LOOKBACK_DAYS" -DefaultValue "7"
$CorsOrigin = Resolve-Setting -CurrentValue $CorsOrigin -Settings $configValues -Key "CORS_ORIGIN" -DefaultValue "*"

if (-not $ProjectId) {
    throw "GCP_PROJECT_ID is required. Set it in config\.env or as an environment variable before running this script."
}

$serviceAccountEmail = "$ServiceAccountName@$ProjectId.iam.gserviceaccount.com"
$sourceDir = $repoRoot
$imageName = "$Region-docker.pkg.dev/$ProjectId/$RepositoryName/smart-irrigation-dashboard"
$imageTag = "latest"
$fullImageTag = "$imageName`:$imageTag"

Write-Section "Smart Irrigation Advisor - Cloud Run + Cloud Functions Deployment"
Write-Host "Project ID: $ProjectId"
Write-Host "Region: $Region"
Write-Host "BigQuery Dataset: $Dataset"
Write-Host "Cloud Run Image: $fullImageTag"
Write-Host ""

# 1. Set active project
Write-Section "Setting active project to $ProjectId"
Invoke-Checked { gcloud config set project $ProjectId } "Failed to set active project" | Out-Host

# 2. Enable required APIs
Write-Section "Enabling required Google Cloud APIs"
Invoke-Checked {
    gcloud services enable `
    cloudfunctions.googleapis.com `
    cloudscheduler.googleapis.com `
    pubsub.googleapis.com `
    bigquery.googleapis.com `
    bigquerystorage.googleapis.com `
    cloudbuild.googleapis.com `
    run.googleapis.com `
    artifactregistry.googleapis.com `
    containerregistry.googleapis.com `
    --project $ProjectId
} "Failed to enable required Google Cloud APIs" | Out-Host

# 3. Create or verify service account
Write-Section "Setting up service account $serviceAccountEmail"
$serviceAccountExists = $false
try {
    Invoke-Checked { gcloud iam service-accounts describe $serviceAccountEmail --project $ProjectId } "Service account not found" | Out-Null
    $serviceAccountExists = $true
} catch {
    $serviceAccountExists = $false
}

if (-not $serviceAccountExists) {
    Invoke-Checked {
        gcloud iam service-accounts create $ServiceAccountName `
        --display-name "Smart Irrigation Advisor SA" `
        --project $ProjectId
    } "Failed to create service account" | Out-Host
}

# 4. Grant IAM roles
Write-Section "Granting IAM roles to service account"
$roles = @(
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/pubsub.publisher",
    "roles/pubsub.subscriber",
    "roles/cloudfunctions.invoker",
    "roles/run.invoker",
    "roles/artifactregistry.writer"
)

foreach ($role in $roles) {
    try {
        Invoke-Checked {
            gcloud projects add-iam-policy-binding $ProjectId `
            --member "serviceAccount:$serviceAccountEmail" `
            --role $role `
            --quiet `
            --format="none" 2>$null
        } "Failed to grant $role" | Out-Host
    } catch {
        Write-Host "Role $role might already be assigned (continuing...)"
    }
}

# 5. Create Artifact Registry repository
Write-Section "Creating Artifact Registry repository"
try {
    Invoke-Checked { 
        gcloud artifacts repositories describe $RepositoryName --location $Region --project $ProjectId 
    } "Repository not found" | Out-Null
} catch {
    Invoke-Checked {
        gcloud artifacts repositories create $RepositoryName `
        --repository-format=docker `
        --location=$Region `
        --description="Smart Irrigation Advisor Docker images" `
        --project $ProjectId
    } "Failed to create Artifact Registry repository" | Out-Host
}

# 6. Build Docker image using Cloud Build
Write-Section "Building Docker image using Cloud Build"
Invoke-Checked {
    gcloud builds submit . `
    --tag $fullImageTag `
    --region $Region `
    --project $ProjectId
} "Failed to build Docker image" | Out-Host

Write-Host "✓ Docker image built and pushed to Artifact Registry"

# 7. Create BigQuery dataset
Write-Section "Creating BigQuery dataset $Dataset"
$datasetExists = $false
try {
    $datasetList = Invoke-Checked { bq ls --project_id=$ProjectId } "Unable to list BigQuery datasets"
    if ($datasetList -match [regex]::Escape($Dataset)) {
        $datasetExists = $true
    }
} catch {
    $datasetExists = $false
}

if (-not $datasetExists) {
    Invoke-Checked {
        bq --location=$BigQueryLocation mk --dataset --description="Smart Irrigation Advisor - weather `& recommendations" "$ProjectId`:$Dataset"
    } "Failed to create BigQuery dataset" | Out-Host
} else {
    Write-Host "BigQuery dataset $Dataset already exists"
}

# 8. Create Pub/Sub topic and subscription
Write-Section "Creating Pub/Sub topic and subscription"
try {
    Invoke-Checked { gcloud pubsub topics describe $Topic --project $ProjectId } "Topic not found" | Out-Null
} catch {
    Invoke-Checked { gcloud pubsub topics create $Topic --project $ProjectId } "Failed to create Pub/Sub topic" | Out-Host
}

try {
    Invoke-Checked { gcloud pubsub subscriptions describe $Subscription --project $ProjectId } "Subscription not found" | Out-Null
} catch {
    Invoke-Checked {
        gcloud pubsub subscriptions create $Subscription `
        --topic $Topic `
        --ack-deadline 60 `
        --message-retention-duration 7d `
        --project $ProjectId
    } "Failed to create Pub/Sub subscription" | Out-Host
}

# 9. Deploy Flask Dashboard to Cloud Run
Write-Section "Deploying Flask Dashboard to Cloud Run"
$envVars = @(
    "GCP_PROJECT_ID=$ProjectId",
    "BIGQUERY_DATASET=$Dataset",
    "BIGQUERY_LOCATION=$BigQueryLocation",
    "PUBSUB_TOPIC_ID=$Topic",
    "SENDGRID_API_KEY=$SendGridApiKey",
    "ALERT_FROM_EMAIL=$AlertFromEmail",
    "ALERT_MIN_URGENCY=$AlertMinUrgency",
    "WEATHER_LOOKBACK_DAYS=$WeatherLookbackDays",
    "CORS_ORIGIN=$CorsOrigin"
) -join ","

Invoke-Checked {
    gcloud run deploy $CloudRunServiceName `
    --image $fullImageTag `
    --platform managed `
    --region $Region `
    --memory $CloudRunMemory`MB `
    --cpu $CloudRunCpu `
    --max-instances $CloudRunMaxInstances `
    --allow-unauthenticated `
    --service-account $serviceAccountEmail `
    --set-env-vars $envVars `
    --project $ProjectId
} "Failed to deploy to Cloud Run" | Out-Host

# 10. Get Cloud Run service URL
Write-Section "Retrieving Cloud Run service URL"
$cloudRunUrl = Invoke-Checked {
    gcloud run services describe $CloudRunServiceName --region $Region --format 'value(status.url)' --project $ProjectId
} "Failed to get Cloud Run service URL"
Write-Host "Cloud Run Service URL: $cloudRunUrl"

# 11. Deploy Cloud Functions for background jobs
Write-Section "Deploying Cloud Functions for background jobs"

$sourceDir = Join-Path $repoRoot "src"
$requirementsPath = Join-Path $sourceDir "requirements.txt"

if (-not (Test-Path $requirementsPath)) {
    throw "Missing $requirementsPath. Cloud Functions deployment needs requirements.txt inside the src directory."
}

function Deploy-Function {
    param(
        [string]$Name,
        [string]$EntryPoint,
        [string]$Description
    )

    Invoke-Checked {
        gcloud functions deploy $Name `
            --gen2 `
            --runtime $Runtime `
            --region $Region `
            --source $sourceDir `
            --entry-point $EntryPoint `
            --trigger-http `
            --allow-unauthenticated `
            --service-account $serviceAccountEmail `
            --memory $Memory `
            --timeout $Timeout `
            --min-instances $MinInstances `
            --max-instances $MaxInstances `
            --set-env-vars $envVars `
            --project $ProjectId
    } "Failed to deploy Cloud Function $Name" | Out-Host
}

Deploy-Function -Name "fetch-and-store-weather" -EntryPoint "fetch_and_store_weather" -Description "Smart Irrigation - fetch NASA POWER weather data"
Deploy-Function -Name "evaluate-and-recommend" -EntryPoint "evaluate_and_recommend" -Description "Smart Irrigation - run FAO-56 rule engine and send alerts"
Deploy-Function -Name "get-recommendations" -EntryPoint "get_recommendations" -Description "Smart Irrigation - HTTP API for recommendations"

# 12. Get Cloud Function URLs
Write-Section "Retrieving Cloud Function URLs"
$weatherUrl = Invoke-Checked { gcloud functions describe "fetch-and-store-weather" --region $Region --gen2 --format 'value(serviceConfig.uri)' --project $ProjectId } "Failed to resolve fetch-and-store-weather URL"
$evaluateUrl = Invoke-Checked { gcloud functions describe "evaluate-and-recommend" --region $Region --gen2 --format 'value(serviceConfig.uri)' --project $ProjectId } "Failed to resolve evaluate-and-recommend URL"
$recsUrl = Invoke-Checked { gcloud functions describe "get-recommendations" --region $Region --gen2 --format 'value(serviceConfig.uri)' --project $ProjectId } "Failed to resolve get-recommendations URL"

# 13. Configure Cloud Scheduler jobs
Write-Section "Configuring Cloud Scheduler jobs"

function Create-Or-Update-SchedulerJob {
    param(
        [string]$JobName,
        [string]$Schedule,
        [string]$Uri,
        [string]$Description
    )

    try {
        Invoke-Checked { gcloud scheduler jobs describe $JobName --location $Region --project $ProjectId } "Job not found" | Out-Null
        Invoke-Checked {
            gcloud scheduler jobs update http $JobName `
            --location $Region `
            --schedule $Schedule `
            --time-zone $SchedulerTimezone `
            --uri $Uri `
            --http-method POST `
            --message-body '{}' `
            --oidc-service-account-email $serviceAccountEmail `
            --project $ProjectId
        } "Failed to update scheduler job $JobName" | Out-Host
    } catch {
        Invoke-Checked {
            gcloud scheduler jobs create http $JobName `
            --location $Region `
            --schedule $Schedule `
            --time-zone $SchedulerTimezone `
            --uri $Uri `
            --http-method POST `
            --message-body '{}' `
            --oidc-service-account-email $serviceAccountEmail `
            --description $Description `
            --project $ProjectId
        } "Failed to create scheduler job $JobName" | Out-Host
    }
}

Create-Or-Update-SchedulerJob -JobName "irrigation-fetch-weather" -Schedule $WeatherCron -Uri $weatherUrl -Description "Trigger daily NASA POWER weather fetch"
Create-Or-Update-SchedulerJob -JobName "irrigation-evaluate" -Schedule $EvaluateCron -Uri $evaluateUrl -Description "Trigger daily irrigation rule evaluation and alerts"

# 14. Summary
Write-Section "Deployment Complete!"
Write-Host ""
Write-Host "┌─────────────────────────────────────────────────────────────┐"
Write-Host "│ Smart Irrigation Advisor - Deployment Summary              │"
Write-Host "├─────────────────────────────────────────────────────────────┤"
Write-Host "│ Project ID:                    $ProjectId"
Write-Host "│ Region:                        $Region"
Write-Host "│                                                             │"
Write-Host "│ 📊 DASHBOARD (Cloud Run):                                   │"
Write-Host "│    Service:    $CloudRunServiceName"
Write-Host "│    URL:        $cloudRunUrl"
Write-Host "│                                                             │"
Write-Host "│ ⚙️  CLOUD FUNCTIONS:                                        │"
Write-Host "│    Weather Fetch:    $weatherUrl"
Write-Host "│    Rule Evaluation:  $evaluateUrl"
Write-Host "│    Recommendations:  $recsUrl"
Write-Host "│                                                             │"
Write-Host "│ 📅 SCHEDULER JOBS:                                          │"
Write-Host "│    Weather Fetch:    $WeatherCron (UTC)"
Write-Host "│    Evaluation:       $EvaluateCron (UTC)"
Write-Host "│                                                             │"
Write-Host "│ 🗄️  BIGQUERY DATASET:                                       │"
Write-Host "│    Dataset:          $Dataset ($BigQueryLocation)"
Write-Host "│                                                             │"
Write-Host "└─────────────────────────────────────────────────────────────┘"
Write-Host ""
Write-Host "✅ All systems deployed successfully!"
Write-Host ""
Write-Host "📱 Visit your dashboard: $cloudRunUrl"
Write-Host ""
