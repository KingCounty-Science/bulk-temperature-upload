Water Temperature Bulk Upload Program
Runs bulk upload on subfoulders of bulk_upload
Each subfoulder corresponds to a site, with the site name being the name of the foulder
- right now the 'site name' has to be exactly as it appears in gDATA (re: caps); this will be fixed down the road
- a filtering algorithm looks at the first 12 and last 12 records (3 hours) and removes data outside 1 standard deviation
- linear interoplation fills in these values

a session column tracks individual files, and will be in order the files are uploaded in the foulder, not necessarly chronological

# outputs a metadata foulder with graph and copy of the merged dataframe

# updates existing 15 minute gDATA data.  The update query is probably safer but a bit slower.
# updates daily table based on data in gData


### current error handeling involves adding the error message to the file name.  This has its advantages and disadvantages
