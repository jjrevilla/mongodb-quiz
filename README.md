# mongodb-quiz
MongoDB client that connects to an existing MongoDB  Server and populates a collection from an external CSV file.

**INSTALL**

It's necessary install pip:

```sh
$ apt-get install python-pip
```
Now just install the dependencies:
```sh
$ pip instal -r requirements.txt
```

Now just run the program:
```sh
$ python fill_mongodb.py path='/file_path/to/import'
```

### Note
The program run also two samples sets of data. The first is data obtained from  `twitter api`, storing tweets in `mongodb`; and the second is a sample data to make `map` and `reduce` operations.