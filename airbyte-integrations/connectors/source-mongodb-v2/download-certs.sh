mkdir -p certs

# Extract cert from each server (run from your host machine)
SERVERS=(
  "sg-prod-xap-76934.servers.mongodirector.com"
  "sg-prod-xap-76935.servers.mongodirector.com"
  "sg-prod-xap-76936.servers.mongodirector.com"
  "sg-qa-xap-75833.servers.mongodirector.com"
  "sg-qa-xap-75834.servers.mongodirector.com"
  "sg-qa-xap-75835.servers.mongodirector.com"
  "sg-uat-gel-77306.servers.mongodirector.com"
  "sg-uat-gel-77307.servers.mongodirector.com"
  "sg-uat-gel-77308.servers.mongodirector.com"
  "sg-qa-finance-75879.servers.mongodirector.com"
  "sg-qa-finance-75880.servers.mongodirector.com"
  "sg-qa-finance-75881.servers.mongodirector.com"
  "sg-prod-bookings-76832.servers.mongodirector.com"
  "sg-prod-bookings-76833.servers.mongodirector.com"
  "sg-prod-bookings-76834.servers.mongodirector.com"
  "sg-prod-finance-77192.servers.mongodirector.com"
  "sg-prod-finance-77193.servers.mongodirector.com"
  "sg-prod-finance-77194.servers.mongodirector.com"
)

for server in "${SERVERS[@]}"; do
  echo "Extracting: $server"
  openssl s_client -connect ${server}:27017 -servername ${server} </dev/null 2>/dev/null | \
    openssl x509 -outform PEM > certs/${server}.pem 2>/dev/null
done