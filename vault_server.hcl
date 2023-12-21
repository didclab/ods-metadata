# Enable and configure the Vault database secrets engine
path "auth/token/create" {
  capabilities = ["update"]
}

# Enable the database secrets engine
path "database/*" {
  capabilities = ["create", "update", "delete", "list"]
}

# Configure the database connection
path "database/config/crdb-config" {
  capabilities = ["create", "read", "update", "delete"]
}

# Configure the role for CockroachDB
path "database/roles/crdb-role" {
  capabilities = ["create", "read", "update", "delete"]
}

# Define the policy for the CockroachDB role
path "database/creds/crdb-role" {
  capabilities = ["read"]
}

# Create the role policy
path "sys/policies/acl/roach-client" {
  capabilities = ["create", "read", "update", "delete"]
}

# Create a token with the roach-client policy
path "auth/token/create/roach-client" {
  capabilities = ["update"]
}

# Login using the generated token
path "auth/token/lookup-self" {
  capabilities = ["read"]
}
