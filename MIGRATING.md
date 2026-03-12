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
For example, it took 6 hours each night to transform Five College's Circulation records and over 34 hours over a weekend to transform our Instance records.

4.0.0 has completely rebuilt the transformation logic to utilize SQL native json processing and set-operations.
The transform time is on average 80-95% faster, for example Circulation now takes 20 minutes and Instances take.
While performance is not a "feature" in and of itself we're hoping that operating LDLite will be easier and less stressful.
Personally, the LDLite server has been treated with kid gloves because disruptions can mean a day or week without data.
Notes on migration:
* Parallel execution
* Sizing up Postgres (especially cpu)
* Sizing down the python server

Another positive to come from the rebuild is that the transformation code is no longer a scary black box.
Better data type inference has been a common request which was always "technically" possible but difficult to implement and test.
The new transformation logic includes more accurate datatype detection and high-performance conversion in SQL.
In addition to simpler and faster queries, more accurate datatypes massively reduces the size of the resulting database.
Our database is 40-50% smaller with data stored as the appropriate types.
Notes on migration:
* Processing as text
* Doing casting in certain ways
* Stuff that is ok to leave in?

While not a breaking change, there is one new feature to call out in more detail.
You'll find a new `ldlite_system` schema.
In this schema are some important functions, please do not modify them.
You'll also find a new table load_history_v1 that records runtime information about each load performed.
End users can have direct access to this table but at Five Colleges we've created a view in the public schema that makes it a little more friendly
```sql
CREATE VIEW public.load_history_dashboard AS
SELECT
  table_prefix
  ,folio_path
  ,COALESCE(query_text, 'cql.allRecords=1')
  ,final_rowcount AS rowcount
  ,pg_size_pretty(SUM(t.table_size)) AS total_size
  ,TO_CHAR(data_refresh_start AT TIME ZONE 'America/New_York', 'YYYY-MM-DD HH24:MI') AS data_refresh_start
  ,TO_CHAR(data_refresh_end AT TIME ZONE 'America/New_York', 'YYYY-MM-DD HH24:MI') AS data_refresh_end
FROM ldlite_system.load_history_v1 h
CROSS JOIN LATERAL
(
  SELECT pg_total_relation_size(t."table_schema" || '.' || t."table_name") AS table_size
  FROM INFORMATION_SCHEMA.TABLES t
  WHERE
  (
    h.table_prefix LIKE '%.%' AND
    t.table_schema = SPLIT_PART(h.table_prefix, '.', 1) AND
    t.table_name LIKE (SPLIT_PART(h.table_prefix, '.', -1) || '%')
  ) OR
  (
    h.table_prefix NOT LIKE '%.%' AND
    t.table_name LIKE (h.table_prefix || '%')
  )
) t
GROUP BY 1, 2, 3, 4, 6, 7
```
The new transformation logic is transactional and will not replace the existing tables until it has completely finished transformation.
Because it writes to the load_history_v1 table in the same transaction all changes made in FOLIO up to the data_refresh_start are always gaurenteed to be refleced in the database.
All changes made in FOLIO after the data_refresh_end are gauranteed to not be reflected in the database.
Changes made in FOLIO between the two dates is a schroedinger's situation as LDLite is downloading data during that time and may or may not have picked up the change.

The minimum supported python version is now 3.10, this has been increased from python 3.9 (which became end of life in October 2025).
LDLite will stop supporting python 3.10 when it becames end of life itself in October 2026.

##### Steps to upgrade from 3.X.X
Please refer to the [official python docs](https://docs.python.org/release/3.10.20/using/index.html) for guidance on installing at least python 3.10.
You can use `python3 --version` to check which version you have currently installed.

This new release places more load on the Postgres server and less on the server running python.
One "trick" used to speed up processing was to start multiple ldlite processes in parallel.
This is no longer necessary and may place too much load on the Postgres server.
Only one LDLite process should be running at a time.

For the most part, the datatype changes should not be breaking but there are scenarios where it can be
* Using `TO_TIMESTAMP(metadata_updated_date, 'YYYY-MM-DDTHH24:MI:SS.MS+OF')` instead of `metadata_updated_date::timestamptz`
* Filtering dates as if they were text i.e. `WHERE metadata_updated_date LIKE '2026%'`
* Dividing numeric columns

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
