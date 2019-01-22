# Jobos Bay dataset exploration
Finally back to coding!
This repository is meant to be a "playground" for me to explore the data available for the Jobos Bay.

---
### Data
This repository is based on NERRS data:
http://cdmo.baruch.sc.edu/

Some notes about the NERRS dataset:
I found some issues while playing around with the data.  First, the csv files have whitespace that may result in unexpected issues when querying/inserting data to and from a dataframe or database.  It may also complicate things when trying to load the csv file using certain libraries.  Another interesting issue is that fields with no values are flagged as historical (<4>) instead of missing value (<-2>).

Another issue is that -at least in the meteorological data- there are '-99' values that are not taken into account by the flag values.  It's therefore necessary to filter those values out when working with the data.  Readme has the following to say about those '-99' values...

"In June 2009, in order to repopulate data tables, the Centralized Data Management Office removed all -99999 from SWMP weather data files and replaced them with -99."

---

### Other sources of data for future reference...

https://products.coastalscience.noaa.gov/collections/ltmonitoring/nsandt/data2.aspx

http://cdmo.baruch.sc.edu/dges/

CEAP, USGS and CariCOOS stations.
---
### Potential research topics:

http://drna.pr.gov/wp-content/uploads/2017/08/Final-2017-2022-Jobos-Bay-Management-Plan.pdf

"Develop  field  maps  to  locate  aquifer  points  of  discharge  in  collaboration  with the Research Advisory Committee. 

The Research Advisory Committee recommended initiating this effort by identifying the points  where  changes  in  salinity  have  been  observed,  and  analyzing  historical events that may explain those changes. To date, a reduction in salinity has been observed at  stations 19 and 20. This could indicate seepage, and should be further monitored."

Paper also mentions: Development of an hydrodynamic model, development of a groundwater flow model and research on carbon proccesses for climate change considerations.
