import json
import requests
import pandas as pd

from flask import Flask, request, Response

#constants
TOKEN = '5962201479:AAGJN2qhzKwnevKS02LpHRvWT4gG7QbmHo8'

#info about bot
#https://api.telegram.org/bot5962201479:AAGJN2qhzKwnevKS02LpHRvWT4gG7QbmHo8/getMe

#get updates
#https://api.telegram.org/bot5962201479:AAGJN2qhzKwnevKS02LpHRvWT4gG7QbmHo8/getUpdates

#webhook
#https://api.telegram.org/bot5962201479:AAGJN2qhzKwnevKS02LpHRvWT4gG7QbmHo8/setWebhook?url=https://a1df-2804-7f7-d69a-cab2-7073-9725-ce3b-315d.ngrok-free.app

#send message
#https://api.telegram.org/bot5962201479:AAGJN2qhzKwnevKS02LpHRvWT4gG7QbmHo8/sendMessage?chat_id=6090307949&text=Hi, human


def send_message(chat_id, text):
    url = 'https://api.telegram.org/bot{}/'.format(TOKEN)
    url = url + 'sendMessage?chat_id={}'.format(chat_id)

    r = requests.post(url, json={'text': text})
    print('status code {}'.format(r.status_code))
    
    return None


def load_dataset(store_id):
    #datasets
    df10 = pd.read_csv('D:/comunidade_DS/repos/ds_em_producao/test.csv')
    df_store_raw = pd.read_csv('D:/comunidade_DS/repos/ds_em_producao/store.csv')

    #merge test + store
    df_test = pd.merge(df10, df_store_raw, how='left', on='Store')

    #choose store for prediction
    df_test = df_test.loc[df_test['Store'] == store_id]


    if not df_test.empty:

        #remove closed days/null/coluna 'id'
        df_test = df_test[df_test['Open'] != 0]
        df_test = df_test[~df_test['Open'].isnull()]
        df_test = df_test.drop('Id', axis=1)

        #convert DataFrame to json
        data = json.dumps(df_test.to_dict(orient='records'))

    else:
        data = 'error'

    return data

def predict(data):

    #API Call
    url = 'https://rossmann.herokuapp.com/rossmann/predict'
    header = {'content-type': 'application/json'}
    data = data


    r = requests.post(url, data=data, headers=header)
    print('predict - status code {}'.format(r.status_code))

    d1 = pd.DataFrame(r.json(), columns=r.json()[0].keys())

    return d1


def parse_message(message):
    chat_id = message['message']['chat']['id']
    store_id = message['message']['text']

    store_id = store_id.replace('/', '')

    try:
        store_id = int(store_id)

    except ValueError:
        send_message(chat_id, 'Store ID is wrong')
        store_id = 'error'

    return chat_id, store_id



#api inicialize
app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        message = request.get_json()

        chat_id, store_id = parse_message(message)

        if store_id != 'error':
            #load data
            data = load_dataset(store_id)

            if data != 'error':

                #prediction
                d1 = predict(data)

                #calculate
                d2 = d1[['store', 'predction']].groupby('store').sum().reset_index()

                #send_message
                msg = 'Store number {} will sell ${:,.2f} in the next 6 weeks'.format(
                            d2['store'].values[0],
                            d2['predction'].values[0])
                
                send_message(chat_id, msg)

                return Response('OK', status=200)

            else:
                send_message(chat_id, 'Store not Available')
                return Response('OK', status=200)

        else:
            send_message(chat_id, 'Store ID is wrong')
            return Response('OK', status=200)


    else:
        return '<h1> Rossmann Telegram BOT </h1>'

if __name__ == '__main__':
    port = os.environ.get('PORT', 5000)
    app.run(host='0.0.0.0', port=port)