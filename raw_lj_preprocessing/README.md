# LJ Preprocessing
This is code for loading the LiveJournal dump, converting it into JSON format, and importing it into a PostgreSQL DB.

The main functionality is in `preprocessor.py`, this is used to convert the raw LJ XML data into a cleaned JSON dump.
Run `preprocessor.py` first, then `sentence_tokenize.py` and then