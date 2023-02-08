
config= {"google_api_key":"<Google API Key>",
        "playlist_ID":"<playlist ID to track video stats>",
        "kafka": {
            "bootstrap.servers": "<host-endpoint>",
            "security.protocol": "sasl_ssl",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "<username for cluster>",
            "sasl.password": "<password>",
            },
        "schema_registry": {
            "url": "<Schema registry endpoint >",
            "basic.auth.user.info": "<API key as username>:<API secret as password>",
            }

        }
