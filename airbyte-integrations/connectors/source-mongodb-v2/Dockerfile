FROM airbyte/source-mongodb-v2:dev
USER root

COPY certs/ /tmp/certs/

RUN CACERTS=/etc/pki/java/cacerts && \
    counter=0 && \
    for cert in /tmp/certs/*.pem; do \
      if [ -s "$cert" ]; then \
        echo "Importing: $cert" && \
        keytool -importcert \
          -alias "scalegrid-$counter" \
          -file "$cert" \
          -keystore $CACERTS \
          -storepass changeit \
          -noprompt; \
        counter=$((counter + 1)); \
      fi; \
    done && \
    rm -rf /tmp/certs

USER airbyte