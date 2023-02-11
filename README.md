# Data engineer role - NicaSource

# Documentation


All this part is about how to execute the program and explaining how it works
#### Summary:
- libraries to import
- Command line interface
- General explanation
- Steps I followed to create the code
- Conclusion
 
The github contains:
- [jupyter notebook](https://github.com/Valle120899/NicaSource-Interview/blob/main/DataPipeline.ipynb) 
- [Python code](https://github.com/Valle120899/NicaSource-Interview/blob/main/DataPipeline.py)
- A folder with some [photos](https://github.com/Valle120899/NicaSource-Interview/tree/main/photos)
- The provides [dataset](https://github.com/Valle120899/NicaSource-Interview/blob/main/dataset.csv)


## Libraries to import
First of all you have to install python [here](https://www.python.org/ftp/python/3.11.2/python-3.11.2-amd64.exe), follow the GUI

After that you have to install the next python libraries
```sh
pip install numpy
pip install pandas
pip install warnings
pip install psycopg2
pip install sqlalchemy
pip install argparse
```

## Command line interface
After you install the libraries following the steps below you just have to execute the next line in terminal:
```sh
python DataPipeline.py --User <YourUser> --Pass <YourPassword> --Host <YourHost> --Db <YourDatabase> --Port <YourPort> --Table <Table to create>
```
You have to take in account that if you want to run the code, the **DataPipeline.py** and **dataset.csv** must be in the same path location.

## General explanation
In order to follow all the instructions that were given first of all I have to mention the next points:

- The database provided was created with [Mockaroo](https://www.mockaroo.com) you can see the configuration [here](https://github.com/Valle120899/NicaSource-Interview/blob/main/photos/Dataset%20Schema.png)
- The code was made working with pandas and numpy as big data libraries, another version of the code could have been using pyspark as big data principal library.
- There is a function per any process in the code, so It could be scalable and maintainable
- [Here](https://github.com/Valle120899/NicaSource-Interview/blob/main/photos/results.png) a the expected output


## Steps I followed to create the code
- Investigate about a good dataset that contains the minimum information that was asked
- Create the main function with the first part of the ETL, the extract part, reading the dataset with pandas
- Create a different functions to transform this little dataset with all the requeriments that were asked
- Investigate about how to insert data with good practices in a postgresql database
- split all the functions in order to have a better control and scalability of all of them.
- Test every function in order to join all of them in the final result.
- Write the documentation file


## Conclusion
As I said before in mail and video, I have to repeat it, thank you so much NicaSource Team for the opportunity of continuing the process with me, I hope you like the result!

If you have any question you can mail me
rodrigo.valle4@gmail.com

Best regards,
Rodrigo Valle












