# Step 1 — Create Glue Database
aws glue create-database `
  --database-input file://db.json `
  --region ap-south-1 2>$null

Write-Host "Glue database ready"

# Step 2 — Create Crawler
aws glue create-crawler `
  --cli-input-json file://crawler.json `
  --region ap-south-1 2>$null

Write-Host "Crawler created"

# Step 3 — Start Crawler
aws glue start-crawler `
  --name india-platform-gold-crawler `
  --region ap-south-1

Write-Host "Crawler started... waiting"

# Step 4 — Wait until crawler is READY
do {
    Start-Sleep -Seconds 10
    $status = aws glue get-crawler `
        --name india-platform-gold-crawler `
        --region ap-south-1 `
        --query "Crawler.State" `
        --output text

    Write-Host "Crawler status: $status"
} while ($status -ne "READY")

Write-Host "Crawler finished"

# Step 5 — Create Athena bucket
aws s3api create-bucket `
  --bucket india-platform-athena-results-111315405619 `
  --region ap-south-1 `
  --create-bucket-configuration LocationConstraint=ap-south-1 2>$null

Write-Host "Athena bucket ready"

# Step 6 — Run Athena query
$queryId = aws athena start-query-execution `
  --query-string "SELECT city, temperature_c, overall_risk FROM city_alerts ORDER BY temperature_c DESC" `
  --query-execution-context Database=india_data_platform `
  --result-configuration OutputLocation=s3://india-platform-athena-results-111315405619/ `
  --region ap-south-1 `
  --query "QueryExecutionId" `
  --output text

Write-Host "Query started: $queryId"

Start-Sleep -Seconds 5

aws athena get-query-results `
  --query-execution-id $queryId `
  --region ap-south-1