from pyspark.sql.functions import udf, broadcast
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig
from presidio_analyzer.nlp_engine import NlpEngineProvider
from langdetect import detect
#!python -m spacy download it_core_news_lg
#!python -m spacy download en_core_web_sm
#!python -m spacy download de_core_news_sm
def compute(source_df):
    models = [
        {"lang_code": "it", "model_name": "it_core_news_lg"},
        {"lang_code": "en", "model_name": "en_core_web_sm"},
        {"lang_code": "de", "model_name": "de_core_news_sm"}
    ]
    configuration = {
        "nlp_engine_name": "spacy",
        "models": models
    }
    # Configuration of the NLP Engine
    provider = NlpEngineProvider(nlp_configuration=configuration)
    nlp_engine = provider.create_engine()
    analyzer = AnalyzerEngine(nlp_engine=nlp_engine, supported_languages=['it', 'en', 'de'])
    
    # Get an existing SparkSession or create a new one if none exists
    #spark = SparkSession.builder.appName("analyzer").getOrCreate()
    
    # Broadcast the analyzer instance
    broadcast_analyzer = spark.sparkContext.broadcast(analyzer)
    
    def detect_language(text):
        try:
            language = detect(text)
        except:
            language = "unknown"
        return language
    
    def anonymize_description(description):
        if description is None:
            return
        try:
            language = detect(description)
        except:
            language = "unknown"
        if language in ['it', 'en', 'de']:
            input_language = language
        else:
            input_language = 'en'
        
        analyzer_results = broadcast_analyzer.value.analyze(text=description, language=input_language)
        
        operators = {"DEFAULT": OperatorConfig("replace", {"new_value": "<ANONYMIZED>"})}
        
        anonymizer = AnonymizerEngine(operators=operators)
        
        anonymized_results = anonymizer.anonymize(
            text=description,
            analyzer_results=analyzer_results,
            operators=operators
        )
        
        return anonymized_results.text
    
    anonymize_description_udf = udf(anonymize_description, StringType())
    
    selected_columns = ['Number', 'Description', 'Short_description', 'Resolution_notes']
    result_df = source_df.select(selected_columns)
    
    result_df = result_df.withColumn("Description_anonymized", anonymize_description_udf(result_df["Description"])) \
                         .withColumn("Short_description_anonymized", anonymize_description_udf(result_df["Short_description"])) \
                         .withColumn("Resolution_notes_anonymized", anonymize_description_udf(result_df["Resolution_notes"]))
    
    return result_df