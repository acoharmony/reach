# Databricks Serverless Public IP Notes

Captured: 2026-10-02

## Local Machine

Route check:

```text
1.1.1.1 via 192.168.1.1 dev eth1 src 192.168.1.151 uid 1002
```

Observed public IPs from this machine:

```text
api.ipify.org          68.134.99.31
checkip.amazonaws.com 68.134.99.31
ifconfig.me           136.226.52.200
```

Local interface IPs observed:

```text
192.168.1.151
172.19.0.1
172.18.0.1
172.17.0.1
```

## Databricks Serverless Probe

Observed from Databricks serverless notebook task:

```text
hostname  dbletG5JQNV
local IP  10.32.0.240
```

Observed public IPs by echo service:

```text
api.ipify.org          40.116.85.133
checkip.amazonaws.com 40.116.85.129
ipv4.icanhazip.com    40.116.85.128
v4.ident.me           40.116.85.132
ifconfig.me           40.116.85.128
```

Earlier one-off serverless probe:

```text
hostname  dbletLCXV62
local IP  10.48.10.51
public IP 172.183.84.6
```

## Databricks Shared Utility Cluster Probe

Cluster:

```text
name       Shared Utility Cluster
cluster_id 0526-212218-gu6aa8w2
mode       classic shared / user isolation
```

Observed from notebook task on the existing cluster:

```text
hostname  0526-212218-gu6aa8w2-10-128-176-132
local IP  10.128.176.132
```

Observed public IP by echo service:

```text
api.ipify.org          157.55.141.248
checkip.amazonaws.com 157.55.141.248
ipv4.icanhazip.com    157.55.141.248
ifconfig.me           157.55.141.248
v4.ident.me           timed out
```

## Official Databricks Published Range Match

Source:

```text
https://www.databricks.com/networking/v1/ip-ranges.json
```

Downloaded JSON metadata:

```text
timestampSeconds 1788759480
timestamp UTC    2026-09-07T05:38:00+00:00
schemaVersion    1.0
```

Matched record:

```text
platform azure
region   northcentralus
service  Databricks
type     outbound
```

Current published IPv4 prefixes for that record:

```text
135.232.159.16/28
135.232.159.32/29
135.232.171.192/26
172.183.56.32/29
172.183.73.8/29
172.183.74.16/29
172.183.80.240/29
172.183.84.0/29
20.125.222.168/29
20.161.68.200/29
20.161.68.208/29
20.161.81.88/29
20.161.82.48/29
20.36.151.208/28
20.65.4.240/28
23.101.152.95/32
40.116.81.168/29
40.116.84.80/29
40.116.85.128/28
40.70.144.208/28
40.80.188.0/28
52.162.20.0/23
52.240.210.16/28
68.154.4.136/29
74.249.107.232/29
```

Observed serverless IP mappings:

```text
172.183.84.6    -> 172.183.84.0/29
40.116.85.128   -> 40.116.85.128/28
40.116.85.129   -> 40.116.85.128/28
40.116.85.132   -> 40.116.85.128/28
40.116.85.133   -> 40.116.85.128/28
```

Note: Databricks documentation says the published serverless outbound IP list can change over time, so allowlists should be refreshed from the Databricks JSON rather than hard-coded permanently.
