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
    [string]$CorsOrigin = $env:CORS_ORIGIN
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

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

    $output = & $Command
    if ($LASTEXITCODE -ne 0) {
        throw $FailureMessage
    }

    return $output
}

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
    throw "GCP_PROJECT_ID is required. Set it in config\\.env or as an environment variable before running this script."
}

$serviceAccountEmail = "$ServiceAccountName@$ProjectId.iam.gserviceaccount.com"
$sourceDir = Join-Path $repoRoot "src"
$requirementsPath = Join-Path $sourceDir "requirements.txt"

if (-not (Test-Path $requirementsPath)) {
    throw "Missing $requirementsPath. Cloud Functions deployment needs requirements.txt inside the source directory."
}

Write-Section "Setting active project to $ProjectId"
Invoke-Checked { gcloud config set project $ProjectId } "Failed to set active project to $ProjectId" | Out-Host

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
    --project $ProjectId
} "Failed to enable required Google Cloud APIs" | Out-Host

Write-Section "Creating service account $serviceAccountEmail"
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
    } "Failed to create service account $serviceAccountEmail" | Out-Host
}

Write-Section "Granting IAM roles to $serviceAccountEmail"
$roles = @(
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/pubsub.publisher",
    "roles/pubsub.subscriber",
    "roles/cloudfunctions.invoker",
    "roles/run.invoker"
)

foreach ($role in $roles) {
    Invoke-Checked {
        gcloud projects add-iam-policy-binding $ProjectId `
        --member "serviceAccount:$serviceAccountEmail" `
        --role $role `
        --quiet
    } "Failed to grant $role to $serviceAccountEmail" | Out-Host
}

Write-Section "Creating BigQuery dataset $Dataset in $BigQueryLocation"
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
        bq --location=$BigQueryLocation mk --dataset --description="Smart Irrigation Advisor - weather & recommendations" "$ProjectId`:$Dataset"
    } "Failed to create BigQuery dataset $Dataset" | Out-Host
}

Write-Section "Creating Pub/Sub topic and subscription"
try {
    Invoke-Checked { gcloud pubsub topics describe $Topic --project $ProjectId } "Topic not found" | Out-Null
} catch {
    Invoke-Checked { gcloud pubsub topics create $Topic --project $ProjectId } "Failed to create Pub/Sub topic $Topic" | Out-Host
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
    } "Failed to create Pub/Sub subscription $Subscription" | Out-Host
}

Write-Section "Deploying Cloud Functions"
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

$weatherUrl = Invoke-Checked { gcloud functions describe "fetch-and-store-weather" --region $Region --gen2 --format="value(serviceConfig.uri)" --project $ProjectId } "Failed to resolve fetch-and-store-weather URL"
$evaluateUrl = Invoke-Checked { gcloud functions describe "evaluate-and-recommend" --region $Region --gen2 --format="value(serviceConfig.uri)" --project $ProjectId } "Failed to resolve evaluate-and-recommend URL"
$recsUrl = Invoke-Checked { gcloud functions describe "get-recommendations" --region $Region --gen2 --format="value(serviceConfig.uri)" --project $ProjectId } "Failed to resolve get-recommendations URL"

Write-Section "Configuring Cloud Scheduler jobs"
try {
    Invoke-Checked { gcloud scheduler jobs describe "irrigation-fetch-weather" --location $Region --project $ProjectId } "Scheduler job missing" | Out-Null
    Invoke-Checked {
        gcloud scheduler jobs update http "irrigation-fetch-weather" `
        --location $Region `
        --schedule $WeatherCron `
        --time-zone $SchedulerTimezone `
        --uri $weatherUrl `
        --http-method POST `
        --message-body '{}' `
        --oidc-service-account-email $serviceAccountEmail `
        --project $ProjectId
    } "Failed to update scheduler job irrigation-fetch-weather" | Out-Host
} catch {
    Invoke-Checked {
        gcloud scheduler jobs create http "irrigation-fetch-weather" `
        --location $Region `
        --schedule $WeatherCron `
        --time-zone $SchedulerTimezone `
        --uri $weatherUrl `
        --http-method POST `
        --message-body '{}' `
        --oidc-service-account-email $serviceAccountEmail `
        --description "Trigger daily NASA POWER weather fetch for all fields" `
        --project $ProjectId
    } "Failed to create scheduler job irrigation-fetch-weather" | Out-Host
}

try {
    Invoke-Checked { gcloud scheduler jobs describe "irrigation-evaluate" --location $Region --project $ProjectId } "Scheduler job missing" | Out-Null
    Invoke-Checked {
        gcloud scheduler jobs update http "irrigation-evaluate" `
        --location $Region `
        --schedule $EvaluateCron `
        --time-zone $SchedulerTimezone `
        --uri $evaluateUrl `
        --http-method POST `
        --message-body '{}' `
        --oidc-service-account-email $serviceAccountEmail `
        --project $ProjectId
    } "Failed to update scheduler job irrigation-evaluate" | Out-Host
} catch {
    Invoke-Checked {
        gcloud scheduler jobs create http "irrigation-evaluate" `
        --location $Region `
        --schedule $EvaluateCron `
        --time-zone $SchedulerTimezone `
        --uri $evaluateUrl `
        --http-method POST `
        --message-body '{}' `
        --oidc-service-account-email $serviceAccountEmail `
        --description "Trigger daily irrigation rule evaluation and alert dispatch" `
        --project $ProjectId
    } "Failed to create scheduler job irrigation-evaluate" | Out-Host
}

Write-Section "Deployment complete"
Write-Host "Cloud Function URLs:"
Write-Host "  fetch-and-store-weather  -> $weatherUrl"
Write-Host "  evaluate-and-recommend   -> $evaluateUrl"
Write-Host "  get-recommendations      -> $recsUrl"
Write-Host ""
Write-Host "Scheduler jobs (UTC):"
Write-Host "  irrigation-fetch-weather -> $WeatherCron"
Write-Host "  irrigation-evaluate      -> $EvaluateCron"
Write-Host ""
Write-Host "Test the API with:"
Write-Host "  curl `"$recsUrl?summary=true`""