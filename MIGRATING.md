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

### 4.0.0 - The "I can't believe that actually worked" Release

This release is the culmination of much theorizing, experimenting, and testing.
It's impossible to convey how excited I am to finally make this release public!

Until now, LDLite expanded the dowloaded JSON row by row using Python.
This worked well enough for small tables but performance really broke down for the largest tables.
For example, it took 6 hours each night to transform Five College's Circulation records and over 34 hours each weekend to transform our Instance records.

4.0.0 has completely rebuilt the transformation logic to utilize SQL native json processing and set-operations.
The transform time is on average 95% faster, for example Circulation now takes 8 minutes and Instances take 1.5 hours.
While performance is not a "feature" in and of itself this change allows for more frequent loads and fresher data.

Another positive to come from the rebuild is that the transformation code is no longer a scary black box.
Better data type inference has been a common request which was always "technically" possible but difficult to implement and test.
The new transformation logic includes more accurate datatype detection and high-performance conversion in SQL.

_Another_ positive is that the data is now replaced transactionally.
If LDLite fails to download or transform the existing data will remain in place (freshness can be checked in the ldlite_system.load_history_v1 table).

SQLite support had to be removed to make this rebuild possible as it wasn't possible to write SQL compatible with all three of postgres, sqlite, and duckdb.
Postgres 14+ and duckdb 1.14+ are now required as they contain features necessary for the rebuild.
These will be supported as long as they are maintained by their respective projects.
One side effect of increasing the minimum duckdb version is that the `select` and `export_csv` methods can now use the built-in functionality.
You will see a change to the output format of both of these methods.

While not a breaking change, there is one new feature to call out in more detail.
You'll find a new `ldlite_system` schema with a table `load_history_v1` that records runtime information about each load performed.
Please see the README.md file for more documentation on this new table.

The minimum supported python version is now 3.10, this has been increased from python 3.9 (which became end of life in October 2025).
LDLite will stop supporting python 3.10 when it becames end of life itself in October 2026.

Lastly, `export_excel` has been removed.

##### Steps to upgrade from 3.X.X
Please refer to the [official python docs](https://docs.python.org/release/3.10.20/using/index.html) for guidance on installing at least python 3.10.
You can use `python3 --version` to check which version you have currently installed.

This new release places more load on the Postgres server and less on the server running python.
One "trick" previously used to speed up transformation was to start 4-6 ldlite processes in parallel.
This is no longer necessary and may place too much load on the postgres server if multiple tables are being transformed at once.
If you have the ability, reallocating resources (especially cpu) to the Postgres server is recommended.
The `work_mem` and `max_parallel_workers_per_gather` settings are also worth looking at.
Because of the transactional replacement, you will need enough free disk space on postgres to store an extra copy of the largest tables.
This is probably Instances, for Five Colleges our Instances are ~50gb and require ~80gb of free disk space to transform.

If you have any issues with the new transform you can pass the `use_legacy_transform` parameter to the query method.
This parameter will stop working in a future major release of LDLite.

For the most part, the datatype changes should not be breaking but there are scenarios where it can be
* Using `TO_TIMESTAMP(metadata_updated_date, 'YYYY-MM-DDTHH24:MI:SS.MS+OF')` instead of `metadata_updated_date::timestamptz`
* Filtering dates as if they were text
  * `WHERE metadata_updated_date LIKE '2026%'` will need to become `WHERE EXTRACT(YEAR FROM metadata_updated_date) = 2026`
* Dividing integer columns by integers
  * `request_queue_size / 5` will need to become `request_queue_size::numeric / 5`

If you were using experimental_connect_sqlite, switch to duckdb and connect_db.
Recent releases of DuckDB are much more stable than in the past when sqlite was provided an alternative.

If you need excel support you can use the [XlsxWriter python library](https://xlsxwriter.readthedocs.io/).
LDLite can still export csvs using the export_csv method which are openable in excel.

## Previous Major Releases

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
