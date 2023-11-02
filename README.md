# Capital Bikeshare Logging
Logging bike and system availability over time in the DC region to postgres.

It's pretty simple, but I wanted a dataset of "real time" availabiliy for the system. A personal gripe is the lack of bikes around Malcolm X park in the mornings!

This should be portable to any bikeshare system run by Lyft, the website with the details in DC is here: https://www.capitalbikeshare.com/system-data.

Currently this runs as a chron job on render, hooked up to a postgres database. We pull the bike station availability, as well as the individual bike status every five minutes and store that in postgres.

## Render
[Render](https://render.com) offers a cheap tier of chron jobs that run at your whim and can use python. They also offer a free tier of postgres available for 90 days -- I recommend paying for a bit more, but it's a great way to get started.

### Chron Job
When setting up the chron job, you can use the variable names in the python script to label your Environment Variables in the render web UI.

I recommend setting the python version to 3.10.0: https://render.com/docs/python-version.

The command to run this script is simple:

```{bash}
$ python data_scraper.py
```