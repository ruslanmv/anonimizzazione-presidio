from pyspark.sql.functions import udf, broadcast
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig
from presidio_analyzer.nlp_engine import NlpEngineProvider
from langdetect import detect
from dotenv import load_dotenv
import re
import os
import findspark

findspark.init()
load_dotenv()


def compute(source_df):
    models = [{"lang_code": "it", "model_name": "it_core_news_lg"},
              {"lang_code": "en", "model_name": "en_core_web_sm"},
              {"lang_code": "de", "model_name": "de_core_news_sm"}]

    configuration = {"nlp_engine_name": "spacy", "models": models}

    # Configuration of the NLP Engine
    provider = NlpEngineProvider(nlp_configuration=configuration)
    nlp_engine = provider.create_engine()
    analyzer = AnalyzerEngine(nlp_engine=nlp_engine,
                              supported_languages=['it', 'en', 'de'])

    # Get an existing SparkSession or create a new one if none exists
    spark = SparkSession.builder \
                        .appName(os.environ["SESSION_NAME"]) \
                        .config("spark.executor.memory", "5g") \
                        .config("spark.driver.memory", "5g") \
                        .getOrCreate()

    # Broadcast the analyzer instance
    broadcast_analyzer = spark.sparkContext.broadcast(analyzer)

    def anonymize_custom(text):
        def anonymize_id(text):
            # Assuming IDs start with a capital letter followed by digits
            id_pattern = r'[A-Z]\d+'
            anonymized_text = re.sub(id_pattern, '<ANONYMIZED>', text)
            return anonymized_text

        def anonymize_ndg_number(text):
            # Assuming NDG numbers are preceded by "ndg" and followed by digits
            ndg_pattern = r'ndg\s+(\d+)'
            anonymized_text = re.sub(ndg_pattern, 'ndg <ANONYMIZED>', text)
            return anonymized_text

        def anonymize_passport_numbers(text, replace_with="<ANONYMIZED>"):
            passport_pattern = r'\b[A-Z0-9]{2}[0-9]{6}\b'
            anonymized_text = re.sub(passport_pattern, replace_with, text)
            return anonymized_text
        def anonymize_large_numbers(text, threshold=1000, replace_with="<ANONYMIZED>"):
             large_number_pattern = r'\b\d{4,}\b' 
             anonymized_text = re.sub(large_number_pattern, lambda match: replace_with if int(match.group(0)) > threshold else match.group(0), text) 
             return anonymized_text         

        # Example usage of anonymization functions
        text = anonymize_passport_numbers(text)
        text = anonymize_ndg_number(text)
        text = anonymize_id(text)
        text = anonymize_large_numbers(text)
        return text

    def anonymize_description_multi(description):
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
        description = anonymize_custom(description)
        analyzer_results = broadcast_analyzer.value.analyze(
            text=description, language=input_language)
        anonymizer = AnonymizerEngine()
        operators = {"DEFAULT": OperatorConfig(
            "replace", {"new_value": "<ANONYMIZED>"})}
        anonymized_results = anonymizer.anonymize(
            text=description, analyzer_results=analyzer_results, operators=operators)
        return anonymized_results.text

    # Register the UDF
    anonymize_description_multi_udf = udf(
        anonymize_description_multi, StringType())
    selected_columns = ['Number', 'Description',
                        'Short_description', 'Resolution_notes']
    result_df = source_df.select(selected_columns)

    # Add the "language" column by applying the UDF to the "Description" column
    result_df = result_df.withColumn("Description_anonymized", anonymize_description_multi_udf(result_df["Description"])) \
                         .withColumn("Short_description_anonymized", anonymize_description_multi_udf(result_df["Short_description"])) \
                         .withColumn("Resolution_notes_anonymized", anonymize_description_multi_udf(result_df["Resolution_notes"]))

    return result_df
