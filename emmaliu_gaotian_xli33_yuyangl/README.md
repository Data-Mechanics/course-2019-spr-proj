# CS504 Project - Mapping Amman

***README.md file has updated for Project #2. Please jump to Project #2 Part for more details.***

***This project is a part of [BU Spark!](http://www.bu.edu/spark/) project “Mapping Amman”.***

## Introduction
Prof.Anderson of BU history department is conducting an ethnography of present-day Amman, Jordan, to determine how 22-35 year olds with university degrees, who are living in representative neighborhoods around the city are navigating the neoliberal development projects financed by the state.  In just the last two decades, Amman has doubled in size, and schooling, jobs and entertainment have been privatized. The youth population is the first generation to be fully immersed in this new city, facing its new obstacles and opportunities. 

2 big topics to guide the project:

- Using the intersection of social media and mapping, where do people spend their time? Is it feasible to get to these places, and are these places conducive to networking and socializing in affordable ways? Are people happy when they are in these places? If not, what are the conditions when they are not happy?

- Can we find a relationship between university degrees, skills, language, and English training centers, and whether or not a person in Amman has a job?

## Prerequisite
To successfully run this project, you need to install these packages:
```sh
$ pip install twitter
```
```sh
$ pip install google-cloud-translate
```
```sh
$ pip install vaderSentiment
```

## Project #1
According to course syllabus, we focus on getting data sets and using them to do transformation in Project #1.

- Scrape data from social media and store them in [MongoDB](https://www.mongodb.com/).
- Do some data transformation.
- Analyze results and discuss them with the project partner.

## Our Work in Project #1
Because our project partener do not provide us with the data, so our work focused more on how to get the useful data for the project. **We have discussed this with Prof. Andrei and he understood this and reduced the requirements for transformation for our Project #1.**

### 1. Scrape Data
Following the two topics mentioned above, out first step is getting data since we are not provided with data sets from the project partner. We decide to get data from [Twitter](https://twitter.com/?lang=en) and [LinkedIn](https://www.linkedin.com/).

- **Twitter**

  At first, we want to use [Twitter API](https://developer.twitter.com/content/developer-twitter/en.html) to get users who live in Amman and then get their tweets. However, the API do not provide this fuction and we cannot directly get all the users with location "Amman" in their profile. Hence, we decide to get tweets with location "Amman". We set the coordinates of the center of Amman and the radius to specify the range and get the tweets in that area. We get 5,000 tweets, store all the information as a [JSON](https://www.json.org/) file and upload it to [the course website](http://datamechanics.io/data/tweets_amman.json). **Due to the execute.py will run all Python file in subdirectory, we upload the source code for getting tweets in crawlTweets.pdf.**

- **LinkedIn**

  According to the project topic, we mainly focus on people who are from Amman (Jordan) and their education as well as job. We use an API called [Linkedin Search
Export](https://phantombuster.com/api-store/3149/linkedin-search-export) to run through people’s profiles on Linkedin. We only get around 150 sets of data and upload it to [the course website](http://datamechanics.io/data/linkedindataset.json) as well. The limitaion will be discussed later.

### 2. Data Transformation
- **Twitter**

1. Filter out the tweets that the user's location is not empty.
2. Do the aggregation transformation on the location in the user's profile
3. Calculate the number of people who posted the tweets within Amman for each location where these people come from.
4. Calculate the average number of followers and friends for users in these location.

- **LinkedIn**

1. Do the aggregation transformation on the query on the dataset.
2. According to the current job, calculate the number of people who changed their jobs.
3. Do the project transformation to get the data we need.

### 3. Analysis and Discussion
- **Analysis for Twitter**

  The data set we get includes some useful information, such as the location of the users. By analyzing that, we find that there are basically two types of people: native and tourists and there are some difference in their number of followers and friends. About 60% of them are tourists. But there are still some people who do not specify their location in their profile. Also, some texts are in Arabic, so we maybe need to translate them into English later.

- **Analysis for LinkedIn**

  We find that there are only a few people who come from Amman in our LinkedIn data set. There are some limitaions related to LinkedIn itself. First, Linkedin only allows up to three degrees of friend relationship to see other people’s profile. As we are both from China and currently studying in US, we do not have lots of friends or know anyone who is from Jordan. Therefore, we do not have deep exposure to view these profiles of people who are from Amman. Second, we can only scrape up to 100 profiles once without premium membership, which would limit our dataset size. Third, some people do not post their educational backgrounds on their Linkedin profiles. It is understandable in some sense that these people probably are not students or recent graduates. Lastly, when we tried to use Amman as query to scrape, some locations in these people’s profiles are neither Jordan or Amman, which is not related to the main purpose of the project. According to http://gs.statcounter.com/social-media-stats/all/jordan, LinkedIn is not a popular social media in Jordan. Therefore, the final results we get is reasonable.

- **Discussion with project partner**

  Based on analysis above, we have a meeting with our project partner on March 7, 2019. We make on agreement that we do not use Linkedin as a sources since there are too many problems in the data set. We will focus on Twitter for our project. Our next goal is to split users who send tweets in Amman into three groups: native, tourist and unknown and try to find more information for each group. If the results is good, we will get more tweets and do sentimental analysis for those tweets.
  
  

## Project #2

According to course syllabus, we focus on problems about data analytics in Project #2.
 
 - Clustering on the place information including in tweets.
 - Sentimental analysis on the text of tweets.
 - Correlation analysis on attributes of user account.
  
## Our Work in Project #2

### 1. Filtering Geo Information
In order to do clustering on the place infomation, first we need to find which tweets include the place information. According to our observation, there are two information that might be useful. The first one is the "place" with a bounding box which provides coordinates. For each bounding box, we calculate the center point to represent it. The second one is the "geo" which directly provides coordinates. According our observation, there are too many overlapping center points calculated from bounding boxes, so this attribute do not provide enough useful information. Therefore, we filter tweets with "geo".

***Note: We do not merge code that using "bounding box" into our master branch. That part of code and result are in LinkedIn branch.***
 
### 2. Clustering by k-means++ Algorithm
In Project #1, we get 5,000 tweets but the number of tweets with "geo" is not enough. To obtain more geo information for clustering, we get all tweets in last 7 days in Amman area. There are about 29,000 tweets in total and about 160 tweets include "geo". We want to use these coordinates to do clustering and find if there are some places that people usually go by solving this optimization problem. The most classic clustering algorithm is k-means. In Project #2, we implement k-means++ algorithm, which is better in initializing cluster centers. When k is set to 3, we get the result as follows.

<div align=center><img src="https://github.com/feiyue33/course-2019-spr-proj/blob/master/emmaliu_gaotian_xli33_yuyangl/image/kmeans_result.jpeg" width="480" height=360"/></div>
 
***Note: If you run the code in trial mode, only about 20 coordinates will be used in clustering.***
 
### 3. Text Translation
Since many tweets in data set are in Arabic, we need to translate Arabic to English because it is much more convenient and efficient to do sentimental analysis on English. We use [Google Cloud Translation API](https://cloud.google.com/translate/docs/apis) to do translation. For 5,000 tweets, it takes about more than 1 hour to translate. We upload the new translated data set to http://datamechanics.io/data/tweets_translated.json.
 
***Note: The trial mode does not include text translation. Please run the code in trial mode if you do not want to do translation.***
 
### 4. Sentimental Analysis
After translation, we do sentimental analysis on tweets in Amman area. We randomly sample 200 tweets from translated data set. Each tweet will get a score from range [-1, 1]. The more the score close to -1, the more negative the tweet is; the more the score close to 1, the more positive the tweet is. We draw a scatter plot to show the results more intuitively.

<div align=center><img src="https://github.com/feiyue33/course-2019-spr-proj/blob/master/emmaliu_gaotian_xli33_yuyangl/image/sentiment_result.jpeg" width="480" height="360"/></div>

### 5. Computation of Correlation Coefficient
In this part, we use two attributes of Twitter user - the number of followers this user has (followers_count) and the number of public lists this user is a member of (listed_count). We compute the correlation coefficient and the p-value of these two attributes. According to our computation results, the correlation coefficient equals to 0.86 and the p-value is close to 0. Therefore, we can conclude that the correlation between followers_count and listed_count is very strong.

## Reference
 - https://developer.twitter.com/en/docs.html
 - https://www.json.org/
 - http://cs-people.bu.edu/lapets/504/
 - https://en.wikipedia.org/wiki/K-means%2B%2B
 - https://cloud.google.com/translate/docs/apis
 - https://github.com/cjhutto/vaderSentiment
