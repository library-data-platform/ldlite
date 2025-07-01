# Migrating to a newer version of LDLite

LDLite follows the [Semantic Versioning](https://semver.org/).
For the most part, it should be safe to upgrade LDLite to the latest MINOR and PATCH versions.
This guide is intended to be for MAJOR version updates.
Please consult the documentation for your package manager of choice to understand how to receive minor updates automatically.

To check your existing ldlite version use
```
python -m pip freeze | grep ldlite
```
or the equivalent command for your package manager.
If you'd like support or assistance upgrading please feel free to reach out to ldlite-support@fivecolleges.edu or the #folio-ldlite channel in Slack.

## Latest Major Release

### 3.0.0 - The Prep for Takeoff Release

The good: LDLite has been running unchanged in production for multiple years!

The bad: No one really knows how to safely make fixes and improvements to LDLite.

This release adds a number of Code Quality tools so that improvements can be made safely to LDLite.
For the most part, this release has no behavior changes but there are some breaking changes with how LDLite is installed.


The minimum supported python version is now 3.9, this has been increased from python 3.7 (which became end of life in June 2023).
LDLite will stop supporting python 3.9 when it becames end of life itself in October 2025.

Additionally, the LDLite's dependencies have been unpinned and the unused direct dependcency on pandas has been removed.

##### Steps to upgrade from 2.0.0

Please refer to the [official python docs](https://docs.python.org/release/3.9.23/using/index.html) for guidance on installing at least python 3.9.
You can use `python3 --version` to check which version you have currently installed.

When you install ldlite 3.0.0 some of your transient dependencies might automatically get upgraded like requests or numpy.
If you rely on pandas without having a direct dependency on it you may have to re-install it.
Please make sure that any code using ldlite as a dependency is tested.

## Previous Major Releases

### 2.0.0 - The Sunflower Release

All deprecated methods on the LDLite object related to okapi are removed.
If you have not upgraded to 1.0.0, please do that first by following the instructions in the Previous Major Releases section.

##### Steps to upgrade from 1.0.0

None! If you've followed the instructions for migrating to 1.0.0 you're already using the appropriate methods.
If you do encounter new issues after upgrading to 2.0.0, please reach out to ldlite-support@fivecolleges.edu or the #folio-ldlite channel in Slack.

### 1.0.0 - The Sunflower Ready Release

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
