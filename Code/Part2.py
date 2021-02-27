#!/usr/bin/env python
from pyspark.sql import SparkSession, Row
from pyspark import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
import sys
import re

sc = spark._sc
#SQL Context
sqlContext = SQLContext(sc)
print_DIR = "C:/Users/anike/Desktop/Education/George Mason University/Sem 4-Fall - 2020/CS 657 - Mining Massive Datasets/Assignments/Assignment 1/output/"
working_DIR = "C:/Users/anike/Desktop/Education/George Mason University/Sem 4-Fall - 2020/CS 657 - Mining Massive Datasets/Assignments/Assignment 1/speeches/"
files = os.listdir(working_DIR)
files = list(map(lambda x: "file:///" + working_DIR + x,files))
input1 = sc.wholeTextFiles(','.join(files))

def seperate_into_sentences(x):
    y = re.sub('\r','',x)
    y = y.split("\n")
    def extract_data_of__p_tag(y):
        first,last = 0,0
        for line in range(len(y)):
            if y[line].strip() == "<p>":
                first = line
                break
        for i in range(first+1,len(y)):
            if y[i].strip() == "</p>":
                last = i
        return first, last
    i,j = extract_data_of__p_tag(y)
    def remove_p_tags(y):
        x = " ".join(y)
        x = re.sub('<p>','',x)
        x = re.sub('</p>','',x)
        x = " ".join(x.split())
        return x
    y = remove_p_tags(y[i:j])
    y = y.split(".")
    return y

sentences = input1.mapValues(seperate_into_sentences)

# Seperate the values into key, value pairs again
def seperate_key_values(x): return x

sentences = sentences.flatMapValues(seperate_key_values)

def rename_key_files(fileAndWord):
    date = fileAndWord[0].split("/")[-1].split(".")[0][:4]
    return (date, fileAndWord[1])

sentences = sentences.map(rename_key_files)

# Apply lower case to the sentences
sentences = sentences.mapValues(lambda x: x.lower())

def remove_remaining_punctuation(fileAndLine):
    return re.sub('["!()&,:.;/"?]','',fileAndLine)

sentences = sentences.mapValues(remove_remaining_punctuation)

# Remove all stop words

def ignore_stopwords(wordlist):
    pattern = ("lt", "gt", "a", "about","above","after","again","against","all","am","an","and","any","are","aren't","as","at","be","because","been","before","being","below","between","both","but","by","can't","cannot","could","couldn't","did","didn't","do","does","doesn't","doing","don't","down","during","each","few","for","from","further","had","hadn't","has","hasn't","have","haven't","having","he","he'd","he'll","he's","her","here","here's","hers","herself","him","himself","his","how","how's","i","i'd","i'll","i'm","i've","if","in","into","is","isn't","it","it's","its","itself","let's","me","more","most","mustn't","my","myself","no","nor","not","of","off","on","once","only","or","other","ought","our","ours","ourselves","out","over","own","same","shan't","she","she'd","she'll","she's","should","shouldn't","so","some","such","than","that","that's","the","their","theirs","them","themselves","then","there","there's","these","they","they'd","they'll","they're","they've","this","those","through","to","too","under","until","up","very","was","wasn't","we","we'd","we'll","we're","we've","were","weren't","what","what's","when","when's","where","where's","which","while","who","who's","whom","why","why's","with","won't","would","wouldn't","you","you'd","you'll","you're","you've","your","yours","yourself","yourselves")
    def is_in(wordlist):
        return not wordlist in pattern
    wordlist = wordlist.split(" ")
    return " ".join(list(filter(is_in,wordlist)))

sentences = sentences.mapValues(ignore_stopwords)

# Filter if the num of words <=1
def less_than_2_words(fileAndLine):
    if(len(fileAndLine[1].split(" ")) < 2):
        return False
    else:
        return True

sentences = sentences.filter(less_than_2_words)
sentences_for_words = sentences

# Modify the values to be a list of bigrams and sort the order
def create_bigrams(sentence):
    words1  = sentence.split(" ")
    toReturn = []
    for i in range(len(words1)):
        for j in range(i+1, len(words1)):
            pair = list((words1[i],words1[j]))
            pair.sort()
            toReturn.append(tuple(pair))
    return toReturn

sentences = sentences.mapValues(create_bigrams)

sentences = sentences.map(lambda x: x[1]).flatMap(lambda x: x)
# Filter if the length of either word length < 2
def filter_word_length(wordTuple):
    if len(wordTuple[0]) < 2 or len(wordTuple[1]) < 2:
        return False
    else:
        return True

sentences = sentences.filter(filter_word_length)
word_pairs = sentences

sentences = sentences.map(lambda  x: ((x),1))
sentences = sentences.reduceByKey(lambda x,y: x+y)

# filter the values which are less than 6 (5 and below)
def co_occurence_less_than_6(x):
    if x[1] < 6:
        return False
    else:
        return True

sentences = sentences.filter(co_occurence_less_than_6)
sentences = sentences.sortBy(lambda x: x[1], False)


"""
Conditional Probability
"""
# Variable for the sentences is sentences_for_words
words = sentences_for_words
# Split into year and word pairs
def flatten_word_pairs(x): return x.split(" ")

sentences_for_words = sentences_for_words.flatMapValues(flatten_word_pairs)
# Remove the associated year and add a count of 1
count_larger = sentences_for_words.count()
words = sentences_for_words.map(lambda x: (x[1],1))
# Get the count for the words
words = words.reduceByKey(lambda x,y:x+y)
# Get the count for the number of words
count = words.count()

fields_for_counts = [StructField("word",StringType(), True),StructField("count",IntegerType(), True)]
schema_for_counts = StructType(fields_for_counts)
frame_counts = spark.createDataFrame(words, schema_for_counts)

fields_for_pairs = [StructField("word1",StringType(), True),StructField("word2",StringType(), True)]
schema_for_pairs = StructType(fields_for_pairs)
frame_pairs = spark.createDataFrame(word_pairs, schema_for_pairs)
# fake_pairs = sc.parallelize([('house','good'),('country','now'),('united','now'),('public','united'),('house','country'),('house','united'),('official','presents'),('country','public'),('concord','united'),('official','concord'),('house','official'),('good','peace'),('public','good'),])
# fake_pairs = spark.createDataFrame(fake_pairs, schema_for_pairs)
original_pairs = frame_pairs
frame_pairs = original_pairs.withColumnRenamed('word1','word')
join_on_word1 = frame_pairs.join(frame_counts,how='inner',on=['word']).select('word','word2','count')

frame_pairs = original_pairs.withColumnRenamed('word2','word')
join_on_word2 = frame_pairs.join(frame_counts,how='inner',on=['word']).select('word1','word','count')

# Convert back to RDDs
join_on_word1 = join_on_word1.rdd.map(tuple).map(lambda x: ((x[0],x[1]),(x[2])))
join_on_word2 = join_on_word2.rdd.map(tuple).map(lambda x: ((x[0],x[1]),(x[2])))

join_on_word1 = join_on_word1.join(sentences).distinct()
join_on_word2 = join_on_word2.join(sentences).distinct()

join_on_word1_copy = join_on_word1
join_on_word2_copy = join_on_word2

# Calculate the probabilities
prob_b_given_a = join_on_word1_copy.mapValues(lambda x: x[1]/x[0])
prob_a_given_b = join_on_word2_copy.mapValues(lambda x: x[1]/x[0])

# Filter if the probability is less than 0.8
def filter_probability(x):
    if(x[1] <= 0.8):
        return False
    else:
        return True

prob_b_given_a_filtered = prob_b_given_a.filter(filter_probability)
prob_a_given_b_filtered = prob_a_given_b.filter(filter_probability)
prob_b_given_a_filtered = prob_b_given_a_filtered.sortBy(lambda x: x[1], ascending=False)
prob_a_given_b_filtered = prob_a_given_b_filtered.sortBy(lambda x: x[1], ascending=False)

# Calculating LIFT using p(x/y)/p(x)
lift = prob_a_given_b.join(join_on_word1)
lift_copy = lift
lift_copy = lift_copy.mapValues(lambda x: (x[0]*count_larger)/x[1][0])
lift_copy = lift_copy.sortBy(lambda x: x[1], ascending=False)

with open(print_DIR + "Part2-lift.txt","w+") as file_name:
        for x in lift_copy.collect():
            to_write  = str(x[0]) + " " + str(x[1]) + "\n"
            file_name.write(to_write)