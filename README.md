# Mexican Truck Market Monthly Sales Scapper

While developing an analytics project to forecast Mexican truck sales by vehicle segment, I realized there is no public database as thorough as the data that ANPACT (Mexican truck manufacturer's association) publishes in PDF reports every month. Copying and retreiving the data manualy every month was very time-demanding, so I developed the present scrapper app, designed to update my truck sales dataset every month with only 1 click.

The data is then used to validate sales forecasts against observed new data, and if necessary, re-calibrate the models.

Many thanks to [Tabula-py](https://pypi.org/project/tabula-py/) developers for developing the PDF-to-dataframe functionality
