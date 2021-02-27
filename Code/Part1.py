from pyspark import SparkConf, SparkContext
import os
import re

def ignore_punctuation(fileAndLine):
    pattern = "<|>|State of the|Essay|Addresses|Appendices|Return to top|is a project|Brad Borevitz|onetwothree.net|_uacct|urchinTracker|\\t\\t\\tUnion"
    # pattern = "<p>\s*\\r*\\n*\s*([A-Za-z]\w*.*)<\/p>"
    if re.search(pattern, fileAndLine[1]):
        return False
    else:
        return True

def ignore_stopwords(fileAndLine):
    pattern = ("about","above","after","again","against","all","am","an","and","any","are","aren't","as","at","be","because","been","before","being","below","between","both","but","by","can't","cannot","could","couldn't","did","didn't","do","does","doesn't","doing","don't","down","during","each","few","for","from","further","had","hadn't","has","hasn't","have","haven't","having","he","he'd","he'll","he's","her","here","here's","hers","herself","him","himself","his","how","how's","i","i'd","i'll","i'm","i've","if","in","into","is","isn't","it","it's","its","itself","let's","me","more","most","mustn't","my","myself","no","nor","not","of","off","on","once","only","or","other","ought","our","ours","ourselves","out","over","own","same","shan't","she","she'd","she'll","she's","should","shouldn't","so","some","such","than","that","that's","the","their","theirs","them","themselves","then","there","there's","these","they","they'd","they'll","they're","they've","this","those","through","to","too","under","until","up","very","was","wasn't","we","we'd","we'll","we're","we've","were","weren't","what","what's","when","when's","where","where's","which","while","who","who's","whom","why","why's","with","won't","would","wouldn't","you","you'd","you'll","you're","you've","your","yours","yourself","yourselves")
    if fileAndLine[1] in pattern:
        return False
    else:
        return True

def remove_single_space(fileAndLine):
    if fileAndLine[1] == '':
        return False
    else:
        return True

def ignore_2_letters(fileAndLine):
    if len(fileAndLine[1]) < 3:
        return False
    else:
        return True

def split_with_newline(x): return x.split("\r\n")

def remove_trailing_spaces(fileAndLine): return fileAndLine.strip()

def split_with_space(fileAndLine): return fileAndLine.split(" ") 

def split_with_double_hyphen(fileAndLine): return fileAndLine.split("--") 

def remove_remaining_punctuation(fileAndLine):
    return re.sub('["!()&,:.;/"?]','',fileAndLine).lower()

def print_to_file(fil_name, obj, values_only=True):
    with open(working_DIR + fil_name,"w+") as file_name:
        if values_only:
            for x in obj.values().collect():
                file_name.write(x + "\n")
        else:
            for x in obj.collect():
                to_write  = str(x[0]) + "\t" + str(x[1]) + "\n"
                file_name.write(to_write)

def sum_based_on_filename(fileAndCount):
    if fileAndCount[0][0]==fileAndCount[1][0]: 
        x[1]+y[1]

def rename_key_files(fileAndWord):
    date = fileAndWord[0].split("/")[-1].split(".")[0][:4]
    return (date, fileAndWord[1])

conf = SparkConf().setMaster("local")
sc = SparkContext.getOrCreate(conf=conf)
sc.appName = "WordCount"

# CHANGE THE REPOSITORY HERE
# YOU WILL NEED TO RUN EXTRACTHTML.PY BEFORE THIS PROGRAM
print_DIR = "C:/Users/anike/Desktop/Education/George Mason University/Sem 4-Fall - 2020/CS 657 - Mining Massive Datasets/Assignments/Assignment 1/output/"
working_DIR = "C:/Users/anike/Desktop/Education/George Mason University/Sem 4-Fall - 2020/CS 657 - Mining Massive Datasets/Assignments/Assignment 1/speeches/"
files = os.listdir(working_DIR)
files = list(map(lambda x: "file:///" + working_DIR + x,files))
input1 = sc.wholeTextFiles(','.join(files))
# (key, value) = (file_name, files text) 
words = input1.flatMapValues(split_with_newline)
# (key, value) = (file_name, line)

# Filter out html
individual_words = words.filter(ignore_punctuation)

# Remove whitespaces
sentences = individual_words.mapValues(remove_trailing_spaces)

# Split based on space and get the lower case word
sentences_cleaned = sentences.flatMapValues(split_with_space)
# There will be a lot of spaces. Remove this
sentences_cleaned = sentences_cleaned.filter(remove_single_space)



# Remove punctuation
# 1. Split based on --
punctuation1 = sentences_cleaned.flatMapValues(split_with_double_hyphen)
# 2. Remove "!()&,:.;/"? and convert to LOWER CASE
punctuation2 = punctuation1.mapValues(remove_remaining_punctuation)
# 3. Remove stop words
words = punctuation2.filter(ignore_stopwords)
# 3. Remove words with 2 or less letters
words = words.filter(ignore_2_letters)
words = words.map(rename_key_files)

# print_to_file("w.txt", words, values_only=False)

 
"""Computations"""
# 1. Compute the average user of every word over all the years
# Map each tuple <file_name, word> TO <file_name, (word, 1)>
singles = words.map(lambda x: (x[0],(x[1],1)))
single = singles.values()
# TODO MAKE SURE YOU CHANGE THS VALUE TO THE LENGTH OF FILES
average = single.reduceByKey(lambda x,y: x+y).mapValues(lambda x: x/234)
average = average.sortBy(lambda x: x[1], ascending=False)
# print_to_file("average.txt", average, values_only=False)

#2. Compute the maximum and minimum number of times a word appears in all the addresses
singles = words.map(lambda x: ((x[1],x[0]),1))
singles_by_file = singles.groupByKey().mapValues(len)
# map from <(word,filename),count> to <(word, count)>
word_and_count = singles_by_file.map(lambda x: (x[0][0],x[1]))
max_count = word_and_count.reduceByKey(lambda x,y: max(x,y))
min_count = word_and_count.reduceByKey(lambda x,y: min(x,y))

max_count = max_count.sortBy(lambda x: x[1], ascending=False)
min_count = min_count.sortBy(lambda x: x[1], ascending=False)

# 2. Compute the average and standard deviation of times a word appears in a window of 4 years
working_DIR = "C:/Users/anike/Desktop/Education/George Mason University/Sem 4-Fall - 2020/CS 657 - Mining Massive Datasets/Assignments/Assignment 1/speeches-std/"
files = os.listdir(working_DIR)
files = list(map(lambda x: "file:///" + working_DIR + x,files))
input1 = sc.wholeTextFiles(','.join(files))
words = input1.flatMapValues(split_with_newline)
individual_words = words.filter(ignore_punctuation)
sentences = individual_words.mapValues(remove_trailing_spaces)
sentences_cleaned = sentences.flatMapValues(split_with_space)
sentences_cleaned = sentences_cleaned.filter(remove_single_space)
punctuation1 = sentences_cleaned.flatMapValues(split_with_double_hyphen)
punctuation2 = punctuation1.mapValues(remove_remaining_punctuation)
words = punctuation2.filter(ignore_stopwords)
words = words.filter(ignore_2_letters)

def every_fourth1(index):
    years_dict = {1985: 0, 1986: 0, 1987: 0, 1988: 0, 1989: 1, 1990: 1, 1991: 1, 1992: 1,
                    1993: 2, 1994: 2, 1995: 2, 1996: 2, 1997: 3, 1998: 3, 1999: 3, 2000: 3,
                    2001: 4, 2002: 4, 2003: 4, 2004: 4, 2005: 5, 2006: 5, 2007: 5, 2008: 5,
                    2009: 6, 2010: 6, 2011: 6, 2012: 6, 2013: 7, 2014: 7, 2015: 7, 2016: 7,
                    2017: 8, 2018: 8, 2019: 8, 2020: 8}
    return (years_dict[int(index[0])],index[1])

def every_fourth2(index):
    years_dict = {1985: 0, 1986: 0, 1987: 0, 1988: 0, 1989: 1, 1990: 1, 1991: 1, 1992: 1,
                    1993: 2, 1994: 2, 1995: 2, 1996: 2, 1997: 3, 1998: 3, 1999: 3, 2000: 3,
                    2001: 4, 2002: 4, 2003: 4, 2004: 4, 2005: 5, 2006: 5, 2007: 5, 2008: 5,
                    2009: 6, 2010: 6, 2011: 6, 2012: 6, 2013: 7, 2014: 7, 2015: 7, 2016: 7,
                    2017: 8, 2018: 8, 2019: 8, 2020: 8}
    return ((years_dict[int(index[0][0])],index[0][1]),index[1])

def add_year_index(index):
    # date = index[0].split("/")[-1].split(".")[0][:4]
    date = index[0][0]
    years_dict = {1985: 0, 1986: 0, 1987: 0, 1988: 0, 1989: 1, 1990: 1, 1991: 1, 1992: 1,
                    1993: 2, 1994: 2, 1995: 2, 1996: 2, 1997: 3, 1998: 3, 1999: 3, 2000: 3,
                    2001: 4, 2002: 4, 2003: 4, 2004: 4, 2005: 5, 2006: 5, 2007: 5, 2008: 5,
                    2009: 6, 2010: 6, 2011: 6, 2012: 6, 2013: 7, 2014: 7, 2015: 7, 2016: 7,
                    2017: 8, 2018: 8, 2019: 8, 2020: 8}
    return ((years_dict[int(date)],index[0][1]),
            # (index[1]))
            (date,index[1]))

def divide_by_number_samples(index):
    if(index[0][0] == 4):
        return (index[0]),(index[1]/5)
    else:
        return (index[0]),(index[1]/4)

def filter_out_id_9(index):
    if index[0][0] == 9:
        return False
    else:
        return True

def average_2_std_dev(index):
    count = index[1][0]
    avg = index[1][1][1]
    std_dev = index[1][1][0]
    return count > (avg + (2*std_dev))

def account_for_padding(index):
    toModify = list(index)
    if len(index) == 8 or len(index) == 0:
        pass
    else:
        while len(toModify) != 8:
            toModify.insert(0,((index[1][0],0)))
            toModify.insert(0,(index[0]))
    avg = toModify[0]
    years = toModify[1::2]
    total = 0
    for year in years:
        total = total + (avg - year[1])**2
    return total

def map_following_year(index):
    years_dict = {  1985: 9999, 1986: 9999, 1987: 9999, 1988: 9999, 1989: 0, 1990: 9999, 1991: 9999, 1992: 9999,
                    1993: 1, 1994: 9999, 1995: 9999, 1996: 9999, 1997: 2, 1998: 9999, 1999: 9999, 2000: 9999,
                    2001: 3, 2002: 9999, 2003: 9999, 2004: 9999, 2005: 4, 2006: 9999, 2007: 9999, 2008: 9999,
                    2009: 5, 2010: 9999, 2011: 9999, 2012: 9999, 2013: 6, 2014: 9999, 2015: 9999, 2016: 9999,
                    2017: 7, 2018: 9999, 2019: 9999, 2020: 9999}
    return ((years_dict[int(index[0][0])],index[0][1]),index[1])

def remove_9999_year(index):
    if index[0][0] == 9999:
        return False
    else:
        return True

# Removes the file path from the key and leave only the year
words = words.map(rename_key_files)
words2 = words.map(every_fourth1)
singles_2 = words2.map(lambda x: ((x[0],x[1]),1))
singles2_by_file = singles_2.groupByKey().mapValues(len)
# ((bracket, word), count) -> ((0, 'ronald'), 4)
average2 = singles2_by_file.map(divide_by_number_samples)
average2 = average2.sortBy(lambda x: x[1], ascending=False)

# Map the input keys to only the name of the file
# Std Dev over the 4 year bracket

single3 = words.map(lambda x: ((x[0],x[1]),1))
single3 = single3.groupByKey().mapValues(len)
single3 = single3.map(add_year_index)
all_data = average2.join(single3)
# If there is no word in any year, the count would not show up
# Need to account for this below
all_data = all_data.reduceByKey(lambda x,y: x+y)
all_data = all_data.mapValues(account_for_padding)
all_data = all_data.map(divide_by_number_samples)
std_dev2 = all_data.map(lambda x:(x[0],(x[1])**(1/2)))
std_dev2 = std_dev2.sortBy(lambda x: x[1], ascending=False)

# Words from the following bracket's year that has the 
# frequency that exceeds the average plus 2 std dev
avg_std_dev1 = words.map(lambda x: ((x[0],x[1]),1))
avg_std_dev1 = avg_std_dev1.groupByKey().mapValues(len)
# We need to merge this with the <(id, word),(std_dev, average)>
# We can merge the RDD's std_dev2 and average2
avg_std_dev2 = std_dev2.join(average2)
# Map the year to the id for avg_std_dev1
avg_std_dev1  = avg_std_dev1.map(map_following_year)
# Remove the years with 9999
avg_std_dev1 = avg_std_dev1.filter(remove_9999_year)


# Increment the id for all brackets in avg_std_dev2 except the last one
avg_std_dev = avg_std_dev1.join(avg_std_dev2)

# Now filter all the values where the count exceeds the average plus
# two std_dev
avg_std_dev = avg_std_dev.filter(average_2_std_dev)
# <(bracket, word)(count, stddev, avg)>

with open(print_DIR + "Part1-min-max-together.txt","w+") as file_name:
        for x in xx.collect():
            to_write  = str(x[0]) + "\t" + str(x[1][0]) + "\t" + str(x[1][1]) + "\n"
            file_name.write(to_write)


