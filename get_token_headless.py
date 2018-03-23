#!/home/ecom/anaconda3/bin/python

"""
Created on Fri Mar 16 09:38:40 2018
added something to check
@author: Pera
"""

import requests
from selenium import webdriver
import time
from urllib.parse import urlparse
from selenium.webdriver.firefox.options import Options

import hmac
import hashlib


# App token to make first GRAPH_API calls ( app_id | app_secret )

app_token1 = 'your_app_ID|your_app_secret_here'


def genAppSecretProof(access_token, app_secret='your_app_secret_here'):

    '''For given app_secret and user_access_token, it returns appsecret_proof
        parameter, needed for all future graph_api calls'''

    h = hmac.new(
        app_secret.encode('utf-8'),
        msg=access_token.encode('utf-8'),
        digestmod=hashlib.sha256)

    return h.hexdigest()

# %%

# For headless model


def get_user_token():
    try:
        # This link is for calling 'LOGIN-dialog'

        address_ = 'https://www.facebook.com/v2.12/dialog/oauth?client_id=134468323894141&response_type=code&redirect_uri=https://www.facebook.com/connect/login_success.html'

        # We call make an web_driver with Firefox, since we will
        # use it in headless mode

        options = Options()
        options.add_argument('-headless')
        driver = webdriver.Firefox(firefox_options=options)
        # driver = webdriver.Firefox()

        driver.delete_all_cookies()

        driver.get(address_)

        # Find html elements containing ids: email and pass
        email_field = driver.find_element_by_id('email')
        pass_field = driver.find_element_by_id('pass')

        email_field.send_keys('your_e_mail_for_fb')
        pass_field.send_keys('your_password_for_fb')
        pass_field.submit()

        # after we submit username and password, we need
        # to capture the moment of
        # changing url, where we need to find the code
        # That code, later on, we will exchange for access token

        while True:
            time.sleep(0.3)
            if urlparse(driver.current_url).query[:4] == 'code':
                code_for_token = urlparse(driver.current_url).query[5:]
                print(code_for_token)
                break
        # do not forget to change client_id= ..... to your app_id
        address_for_token = 'https://graph.facebook.com/v2.12/oauth/access_token?client_id=134468323894141&redirect_uri=https://www.facebook.com/connect/login_success.html&client_secret=f247b3c6843bab68efb27866cb658245'

        addr_ = address_for_token + '&code=' + code_for_token

        print(addr_)
        access_token = requests.get(addr_, params={'access_token': app_token1}).json()['access_token']

        driver.quit()

    except:

        return 'mistake'

    return access_token


def get_token_appsecret_proof():

    token = get_user_token()

    appsecret_proof = genAppSecretProof(token,'your_app_secret_here')

    return token, appsecret_proof

# %%


def inspect_token(token_for_analysis, app_token=app_token1):
    appsecret_proof = genAppSecretProof('your_app_secret_here',
                                        token_for_analysis)

    adr = 'https://graph.facebook.com/v2.12/debug_token?'
    param = {'input_token': token_for_analysis,
             'access_token': app_token,
             'appsecret_proof': appsecret_proof}

    token_analysis = requests.get(adr, params=param)

    return token_analysis.json()


def get_page_token(page_id, my_personal_token1):

    '''For given page_id, and user_access_token, it returns page_access_token.
    User needs to have admin rights for given page'''

    appsecret_proof1 = genAppSecretProof(my_personal_token1,'your_app_secret_here')

    address_for_getting_page_token = 'https://graph.facebook.com/v2.12/' + \
        page_id + '?fields=access_token'

    page_token = requests.get(address_for_getting_page_token,
                              params={'access_token': my_personal_token1,
                                      'appsecret_proof': appsecret_proof1}).json()['access_token']

    return page_token
