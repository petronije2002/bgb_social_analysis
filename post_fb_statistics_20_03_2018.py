#!/home/ecom/anaconda3/bin/python
"""
Created on Fri Jan 12 10:15:40 2018

@author: Pera
"""

# from nltk.tag import BigramTagger

# import facebook
import pandas as pd
import requests
import datetime
import time
import re
from get_token_headless import get_token_appsecret_proof, get_page_token, genAppSecretProof,inspect_token
from  sending_gmail.send_gmail import send_gmail
from sqlalchemy.orm import sessionmaker

# Import cursors for STG and for production DW database
from get_cursor import get_cursor, get_cursor_dw

conn = get_cursor()



conn_dw = get_cursor_dw()


def truncate_fb_table():
    '''This function truncates table with intermediate results. It is
    nessecery to perfor that, since stored procedure in SQL server, need to
    decide what posts need to be updated'''

    Session = sessionmaker(bind=conn)
    #
    session = Session()
    #
    session.execute('TRUNCATE TABLE dbo.fb_post_stat;')
    session.commit()
    session.close()
    
    
# from get_token_headless import get_page_token

# from get_cursor import get_cursor
# from requests import get
# import ast

# from nltk.corpus import sentiwordnet as swn
# from nltk.tokenize import sent_tokenize,word_tokenize
#
# import nltk
# from nltk.sentiment.vader import SentimentIntensityAnalyzer
# from nltk.corpus import stopwords

# import re, string

# from custom_POS_tagger import custom_tagger

# conn=get_cursor()

# %%

# Here we have a basic settings, that later, we can place in separate file


#list_of_page_ids = ['1793088884353988', '73732120659', '118470104930123',
#                    '213470749040925', '211181072288742', '277102898996105',
#                    '212000445539447', '171716762964322', '186598388089932',
#                    '306374016081548', '314190795715261', 
#                    '638708072824177', '1452426988398959', '164709773540868']

# First we need to get user_access_token nad appsecret_proof,
# to be able to call GRAPH API


my_personal_token, appsecret_proof = get_token_appsecret_proof()

base_url = 'https://graph.facebook.com/v2.12/'

parameters = {'access_token': my_personal_token,
              'appsecret_proof': appsecret_proof}


# %%


def get_all_page_ids_with_tokens():

    # first get the user id by inspecting the token
    user_id = inspect_token(my_personal_token)['data']['user_id']
    print(user_id)
    
    appsecret_proof = genAppSecretProof(my_personal_token)

    address_for_list_of_pages = base_url + user_id + '/accounts'

    list_of_pages_with_tokens = requests.get(address_for_list_of_pages, params={'access_token': my_personal_token,
              'appsecret_proof': appsecret_proof}).json()

#    print(list_of_pages_with_tokens)    

    list_of_pages = {}

    for page in list_of_pages_with_tokens['data']:
#        print(page['id'], page['access_token'])
        list_of_pages[page['id']] = page['access_token']

    # i need to eliminate (in this case only one, page id from the list)

    if '458011101261139' in list_of_pages.keys():
        del list_of_pages['458011101261139']

    return list_of_pages


dictionary_page_id_page_toke = get_all_page_ids_with_tokens()

list_of_page_ids = dictionary_page_id_page_toke.keys()









def get_all_posts_from_page(page_id, page_token1):

    '''function for getting first 25 posts for given page_id'''

    # adress for getting metadata

    adress_ = base_url + page_id + '?metadata=1'

    appsecret_proof = genAppSecretProof(page_token1)
    me = requests.get(adress_, params={'access_token': page_token1,
                                       'appsecret_proof': appsecret_proof})

    link_to_posts = me.json()['metadata']['connections']['posts']

#    print(link_to_posts)

    posts = requests.get(link_to_posts, params={'access_token': page_token1,
                                                'appsecret_proof':appsecret_proof})

    return posts

# %%

# def get_string_dict(list_of_keys):
#
#    for i in range(len(list_of_keys)):
#        list_of_keys[i]='%(' + list_of_keys[i] + ')d'
#
#    return list_of_keys


def make_string(list_of_keys):
    list_of_keys1 = list_of_keys

    string1 = ''
    for i in list_of_keys:
        string1 = string1 + i + ','

    string1 = '(' + string1[0:-1] + ')'

    for i in range(3):
        list_of_keys1[i] = '%(' + list_of_keys1[i] + ')s'

    for i in range(3, len(list_of_keys1)):
        list_of_keys1[i] = '%(' + list_of_keys1[i] + ')d'
    string = ''

    for i in list_of_keys1:
        string = string + i + ','
        print(string)

    string = 'VALUES(' + string[0:-1] + ')'
    return string, string1

# %%


def get_page_posts_statistics(list_of_page_ids, metric_list):
    print(my_personal_token)

    # This function returns all the posts, from all bugaboo pages,
    # after 01-01-2018

    # Here we define the list of features, for each post...post_id,the date
    # it was published, total number of comments, permanent link, and on top of
    # that, we extend that list with the list of metricies we were interested
    # in
    truncate_fb_table()
    
    list_of_keys = ['post_id', 'post_published_time',
                    'total_numer_of_comments',
                    'permanent_link_for_post']

    list_of_keys_from_matric_list = metric_list.split(',')

    list_of_keys = list_of_keys + list_of_keys_from_matric_list

    # intermediate result of every post will be saved in form of dictionary
    # where keys are actually the metricies.

    # We initialize dictionary, with previously defined keys, and zero values

    dictionary_results = dict.fromkeys(list_of_keys, 0)

    # later, we will collect all the posts related to bugaboo FOX.
    # That list, will be saved in DB

    list_of_posts_related_to_fox = []

    # Here we itterate over the list of page_ids, and for each page_id, we
    # browse through all the comments, collect required metricies, until we
    # find the comment published before 01-01-2018, when we go to he next page

    for page_id in list_of_page_ids:

        # At this moment, we have our my_personal_token (user_access_token),
        # end we need to exchange it for page_access_token
        # print('page_id is: ',page_id)
        page_token = dictionary_page_id_page_toke[page_id]
        # page_token = get_page_token(page_id, my_personal_token)
        appsecret_proof = genAppSecretProof(page_token)  

        # Every time, we collect tha data for the post, and put those values
        # in dictionary, we will append that dictionary to the list
        # list_of_results.

        list_of_results = []

        # Get the first 25 posts ( max number of posts you can receive with one
        # graph_api call), from the page_id. Collect all metricies for those
        # posts, and then go to 'next' 25, and so on, until the post published
        # 01-01-2018
        # When you list all possible posts, go to the next page_id...
        # Until the end of the list of page_ids

        first_post_page = get_all_posts_from_page(page_id, page_token)

#        print(first_post_page.json()['data'])

        number_of_posts = 0
        try:
            while True:

                for post in first_post_page.json()['data']:

                    #print(post)

                    number_of_posts += 1

                    time_stamp_of_the_post = post['created_time']

                    time_stamp_object = datetime.datetime.strptime(
                                        time_stamp_of_the_post,
                                        '%Y-%m-%dT%H:%M:%S%z').date()


#                    print(dictionary_results.keys())

#                    print(time_stamp_object)
                    if time_stamp_object < datetime.datetime.strptime('2018-01-01', '%Y-%m-%d').date():
                        print('NASAO SAM STARIJI POST')
                        raise KeyError

                    link_to_post_permalink = base_url + post['id'] + '?fields=permalink_url,message'

                    link_to_post_fields = base_url + post['id'] + '/comments?summary=1'

                    link_to_post_insigths = base_url + post['id'] + '/insights?metric=' + metric_list

                    fields = requests.get(link_to_post_permalink, params={'access_token': page_token,'appsecret_proof':
                                                  appsecret_proof}).json()

                    permanent_link_for_post = fields['permalink_url']

                    message_of_post = fields['message']

                    # Check if the text of post contains the string FOX
                    # and if it does, append that post_id, to the
                    # list_of_posts_related_to_fox

                    if re.search('fox', message_of_post, re.IGNORECASE):
                        list_of_posts_related_to_fox.append({'Post_id':
                                                            post['id'],
                                                            'Product_name':
                                                                'Fox'})

                    post_insights = requests.get(link_to_post_insigths,
                                                 params={'access_token':
                                                 page_token, 'appsecret_proof':
                                                         appsecret_proof})

                    post_fields = requests.get(link_to_post_fields,
                                               params={'access_token':
                                               page_token, 'appsecret_proof':
                                                       appsecret_proof})

                    total_numer_of_comments = post_fields.json()[
                            'summary']['total_count']

                    for result in post_insights.json()['data']:

                        dictionary_results[result['name']] =  result['values'][0]['value']

                    dictionary_results['post_id'] = post['id']

                    dictionary_results['post_published_time'] = time_stamp_of_the_post

                    dictionary_results['total_numer_of_comments'] = total_numer_of_comments

                    dictionary_results['permanent_link_for_post'] = permanent_link_for_post

                    list_of_results.append(dictionary_results)

  
                   
                    dictionary_results=dict.fromkeys(list_of_keys, 0)
#                    conn.executemany('INSERT INTO fb_test.test_table VALUES (%s,%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d)',list_of_results)

            
                
                      
                first_post_page=requests.get(first_post_page.json()['paging']['next'],params={'access_token': page_token,
                                       'appsecret_proof': appsecret_proof})
        
    
                
        
        
        except KeyError:
        
            #post_product_df=pd.DataFrame(list_of_posts_related_to_fox)
            
            #post_product_df.to_sql(name='Post_Product_Relation',con=conn_dw, if_exists= 'replace', index=False)
            
            
            df=pd.DataFrame(list_of_results)
#            df.to_csv(file_name)
#            
            df.to_sql(name='fb_post_stat', con=conn, if_exists = 'append', index=False)

                
           # print("No more posts on this page on this page_id: ", page_id)
            
            dictionary_results=dict.fromkeys(list_of_keys, 0)
        
            
            
#            print(list_of_results)
#            df=pd.DataFrame(list_of_results)
##            df.to_csv(file_name)
##            
#            df.to_sql(name='fb_post_stat', con=conn, if_exists = 'append', index=False)
#
#                
#            print("No more posts on this page on this page_id: ", page_id)
#            
#            dictionary_results=dict.fromkeys(list_of_keys, 0)
        
    
    time.sleep(5)
    connection_dw=conn_dw.raw_connection()
    try: 
        cursor_dw=connection_dw.cursor()
        cursor_dw.callproc('proc_process_fact_facebookposts')
        cursor_dw.close()
        connection_dw.commit()
        
    finally:
        connection_dw.close()
        send_gmail('api.bugaboo@gmail.com','petronije2002@gmail.com','FACT_FacebookPosts from DW Updated','Checking finished')


            
    return list_of_posts_related_to_fox           
            



metric_list_1='post_impressions_organic,post_impressions_organic_unique,post_impressions_viral,post_impressions_viral_unique,' + \
                'post_reactions_like_total,post_reactions_love_total,post_reactions_wow_total,post_reactions_haha_total,' + \
                'post_reactions_sorry_total,post_reactions_anger_total,' + \
              'post_impressions,post_impressions_unique,post_impressions_paid,post_impressions_paid_unique,post_engaged_users,' + \
              'post_negative_feedback,post_negative_feedback_unique,post_consumptions,post_consumptions_unique,' + \
              'post_video_avg_time_watched,post_video_complete_views_organic_unique,post_video_complete_views_paid_unique,post_video_views,post_video_views_unique'
          

#%%
get_page_posts_statistics(list_of_page_ids,metric_list_1)       
                  
#string='post_id varchar(255),post_published_time varchar(255),total_numer_of_comments int,permanent_link_for_post varchar(255),post_impressions_organic int,post_impressions_organic_unique int,post_impressions_viral int,post_impressions_viral_unique int,post_reactions_like_total int,post_reactions_love_total int,post_reactions_wow_total int,post_reactions_haha_total int,post_reactions_sorry_total int,post_reactions_anger_total int,post_impressions int,post_impressions_unique int,post_impressions_paid int,post_impressions_paid_unique int,post_engaged_users int,post_negative_feedback int,post_negative_feedback_unique int,post_consumptions int,post_consumptions_unique int,post_video_avg_time_watched int,post_video_complete_views_organic_unique int,post_video_complete_views_paid_unique int,post_video_views int,post_video_views_unique int'
        
        

    

