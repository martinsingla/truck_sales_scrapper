# Mexican Truck Market Monthly Sales Scapper

While developing a business analytics project on Commercial Vehile (CV) market in Mexico, we found out that there is no historic dataset as complete and thorough enough as the data provided in ANPACT's* monthly reports. Based on that, I developed the present web-scrapping functions which retrieve data from those monthly PDF reports and stores the new data in a Mongo database hosted on Atlas.

There are 3 core functions: (1) to get the data from the online reports. (2) to check if the latest data is already in the cloud database (and if not, store it). (3) To retrieve the latest full dataset from the cloud database.

The datasets are afterwards used for a sales forecasting model, and visualized in an interactive dashboard.

Many thanks to [Tabula-py](https://pypi.org/project/tabula-py/) developers the PDF-to-dataframe functionality, and [PyMongo](https://pymongo.readthedocs.io/en/stable/) for the cloud database and NoSQL querying functionality through Python.

*ANPACT is the Official Mexican Commercial Vehicle Manufacturers' Association
