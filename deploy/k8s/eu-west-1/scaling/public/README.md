1. Install cloudflared

```bash
brew install cloudflared
```

2. Create a tunnel in cloudflare

```bash
TUNNEL_NAME=simon

# Create the tunnel
cloudflared tunnel create ${TUNNEL_NAME}
```

3. Upload the tunnel credentials to k8s

Note that the tunnel create command above will store your credentials in a file at ~/.cloudflared/UUID.json. You can use
this file to create a Kubernetes secret that will be used to authenticate your tunnel to the Cloudflare network. Replace
/path/to/UUID.json with the path to your credentials file.

```bash
# Get the ID of the tunnel
TUNNEL_ID=$(cloudflared tunnel list -o json | jq -r '.[] | select(.name == "simon") | .id')

# Create the secret
kubectl create secret generic tunnel-credentials \
--from-file=credentials.json=$(realpath ~)/.cloudflared/${TUNNEL_ID}.json
```

4. Manage DNS

Create a CNAME record in your DNS configuration that points to the hostname of your tunnel. For example, if your
tunnel's hostname is my-tunnel.cfargotunnel.com, you would create a CNAME record for my-subdomain.example.com that
points to my-tunnel.cfargotunnel.com.

```bash
cloudflared tunnel route dns ${TUNNEL_NAME} ${TUNNEL_NAME}.ubsv.dev
```

5. Deploy the cloudflare config map

```bash
kubectl apply -f cloudflare-configmap.yaml
```

6. Deploy the cloudflare tunnel

```bash
kubectl apply -f cloudflare.yaml
```
