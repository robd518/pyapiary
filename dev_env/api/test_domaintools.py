from pyapiary.api_connectors.domaintools import DomainToolsConnector, AsyncDomainToolsConnector
import asyncio

domaintools = DomainToolsConnector(
    load_env_vars=True,
    trust_env=True,
    verify=False,
    enable_logging=True
)

async_domaintools = AsyncDomainToolsConnector(
    load_env_vars=True,
    trust_env=True,
    verify=False,
    enable_logging=True
)

# ---------- sync ----------
res = domaintools.parsed_whois("domaintools.com")
print(res.status_code)

res = domaintools.reverse_ip("domaintools.com")
print(res.status_code)

res = domaintools.reverse_nameserver("domaintools.net")
print(res.status_code)

params = {"domain": "domaintools.com"}
res = domaintools.iris_investigate(**params)
print(res.status_code)

# ---------- async ----------
async def main():
    res = await async_domaintools.parsed_whois("domaintools.com")
    print(res.status_code)

    res = await async_domaintools.reverse_ip("domaintools.com")
    print(res.status_code)

    res = await async_domaintools.reverse_nameserver("domaintools.net")
    print(res.status_code)

    params = {"domain": "domaintools.com"}
    res = await async_domaintools.iris_investigate(**params)
    print(res.status_code)

asyncio.run(main())