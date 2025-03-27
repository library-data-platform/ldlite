# Migrating a newer version of LDLite

LDLite follows the [Semantic Versioning](https://semver.org/).
For the most part, it should be safe to upgrade LDLite to the latest MINOR and PATCH versions.
This guide is intended to be for MAJOR version updates.
Please consult the documentation for your package manager of choice to understand how to receive minor updates automatically.

To check your existing ldlite version use
```
python -m pip freeze | grep ldlite
```
or the equivalent command for your package manager.
If you'd like support or assistance upgrading please feel free to reach out to ldlite-support@fivecolleges.edu or the #ldlite channel in Slack.

## Latest Major Release

### 1.0.0 - The Sunflower Release

The Sunflower release of FOLIO is bringing some necessary security changes that impact how integrations connect to the API.
1. Refresh Token Rotation was introduced in the Poppy release and will be the only authentication method as of Sunflower.
1. Eureka is a new platform for Auth and Routing using open source technologies, replacing Okapi which is proprietary to FOLIO.

##### Steps to upgrade from 0.0.36 or below

Please upgrade to 0.1.0 first.
You can consult the [tags on the ldlite repository](https://github.com/library-data-platform/ldlite/tags) to see what issues you might encounter.

Upgrade from 0.0.36 to 0.1.0 by running
```
python -m pip install --upgrade 'ldlite==0.1.0'
```
or the equivalent command in your package manager of choice.

##### Steps to upgrade from 0.1.0

First, update all of the places you're connecting to FOLIO to use Refresh Token Rotation auth
```
# change any of these calls
ld.connect_okapi(url="...", tenant="...", user="...", password="...")
ld.connect_okapi(url="...", tenant="...", user="...", password="...", legacy_auth=True)
ld.connect_okapi_token(url="...", tenant="...", token="...")

# to this
ld.connect_okapi(url="...", tenant="...", user="...", password="...", legacy_auth=False)
```
Verify that ldlite continues to function normally.

Once you have made and verified these changes you're ready to upgrade to 1.0.0 by running
```
python -m pip install --upgrade 'ldlite==1.0.0'
```
or the equivalent command in your package manager of choice.

After upgrading, change the places you're connecting to FOLIO to the non-Okapi specific method
```
# change this call
ld.connect_okapi(url="...", tenant="...", user="...", password="...", legacy_auth=False)

# to this
ld.connect_folio(url="...", tenant="...", user="...", password="...")
```
Verify that ldlite continues to function normally.

You're now ready for Sunflower and Eureka! Please note, the url you use to connect to Eureka will change from the one you are using for Okapi.
After upgrading FOLIO you can find the Eureka URL in the same location as the Okapi URL:
> Settings > Software versions > Services > On url

## Previous Major Releases
