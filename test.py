from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql import SparkSession
import findspark
import os
from myprojects.datasets import utils
from dotenv import load_dotenv
findspark.init()
# Create SparkSession

load_dotenv()

os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\Y9GUFM758\\Documents\\GitHub\\anonimizzazione-presidio\\.conda\\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\\Users\\Y9GUFM758\\Documents\\GitHub\\anonimizzazione-presidio\\.conda\\python.exe'


spark = SparkSession.builder.appName(os.environ["SESSION_NAME"]).getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField('Number', StringType(), True),
    StructField('Description', StringType(), True),
    StructField('Short_description', StringType(), True),
    StructField('Resolution_notes', StringType(), True)
])

# Generate the fake descriptions
description1 = '''Si richiede la clonazione anagrafica dell'ndg 9342345 comprensiva dei conti correnti
Il cliente in questone, QUIXA ha richiesto una serie di personalizzazioni che andranno collegate all'NDG ed al conto, per i quali si rendono necessari diversi test in UJ.
Q0 non è fondamentale ma potrebbe essere utile averlo allineato.'''
description2 = '''NEW Customer creation (new ndg) not possible because the customer's passport is already available (see error message)
Customer's passport details: Austrian passport number U223424799
01/16/2018 - 01/15/2028
BH Dornbirn.
Urgent case!
Please research where stored.
Thank you!'''
description3 = '''NEU Kundenanlage nicht möglich, da Reisepass von Kundin bereits vergeben ist (siehe Fehlermeldung)
Reisepassdaten von Kundin: Osterreichischer Reisepass U2723429 16.01.2018 - 15.01.2028 BH Dornbirn.
Dringender Fall!
Bitte um recherche, WO gespeichert Vielen Dank!'''

# Define the invented data
data = [
    ('1', description1, 'Short description 1', 'Resolution notes for number 1'),
    ('2', description2, 'Short description 2', 'Resolution notes for number 2'),
    ('3', description3, 'Short description 3', 'Resolution notes for number 3')
]

# Create the DataFrame
df = spark.createDataFrame(data, schema)

df.show()
df_ano = utils.compute(df)
