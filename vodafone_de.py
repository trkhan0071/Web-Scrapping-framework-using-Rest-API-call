import time

import requests
import json
import os
import sys
from pathlib import Path

PROJECT_LOC = str(Path(os.path.dirname(os.path.realpath(__file__))).parents[1])
sys.path.append(PROJECT_LOC)
from transformation.mobile_scrapper import MobileScrapper


class VodafoneDeScraper(MobileScrapper):
    def __init__(self):
        super().__init__(seller='vodafone.de')
        self.base_url = 'https://www.vodafone.de'
        self.url_category = 'https://www.vodafone.de/privat/handys-tablets-tarife/alle-smartphones.html'
        self.url_api_product = 'https://www.vodafone.de/api/jarvis/v5/hardwareDetails'  # hardwareDetails
        self.payload_data_product = {"offerBtx": "newContract",
                                     "offerSalesChannel": "Online.Consumer",
                                     "subscriptionIds": ["2501", "2502", "2503", "2505"]}
        self.url_api_category = "https://www.vodafone.de/api/jarvis/v2/hardwareOverview?limit=12&start={}"  # hardwareOverview
        self.payload_data_category = {
            "offerBtx": "newContract",
            "offerSalesChannel": "Online.Consumer",
            "subscriptionIds": [
                "2501", "2502", "2503", "2505"
            ]
        }
        self.headers_category = {
            'authority': 'www.vodafone.de',
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'content-type': 'application/json',
            'cookie': 'mdLogger=false; oshop=queryparams||vfchannel||1001; PV=97498552; at_check=true; s_ecid=MCMID%7C53216835864590585531455103526518212422; AMCVS_AE8901AC513145B60A490D4C%40AdobeOrg=1; adv_mf_group=disabled; adv_dc_rtg_cntrl=false; adv_et_entry=www.vodafone.de%2F; _gcl_au=1.1.834670519.1662039681; mat_tel=03eedbac-891a-4ca0-8102-989115488492; et_uk=8efcad2539814e04ae71ca320e7832da; et_gk=76ea79d7f3a849bcb470713a00c85f45|24.09.2022 13:23:00; _fbp=fb.1.1662039681958.809532218; AMCV_AE8901AC513145B60A490D4C%40AdobeOrg=-1124106680%7CMCMID%7C53216835864590585531455103526518212422%7CMCAID%7CNONE%7CMCOPTOUT-1662046882s%7CNONE%7CMCAAMLH-1662644482%7C6%7CMCAAMB-1662644482%7Cj8Odv6LonN4r3an7LhD3WZrU1bUpAkFkkiY1ncBR96t2PTI%7CMCCIDH%7C1623654709%7CvVersion%7C5.2.0%7CMCIDTS%7C19237; htLi=0800%20724%2026%2010; adv_qwt=4%7C0%7C0%7C4%7C0%7C0%7C0%7C0%7C0; kampyleUserSession=1662039684689; kampyleUserSessionsCount=5; kampyleUserPercentile=14.179048343490862; adv_session_pvc=3; adv_values=adv_icmp%3Dop-mofu%3Adialog-mobilfunk%3Abnt%3Acredit%3Agigamobil-red-device-first__ab_mf_0095_web%3Aab%3Ap001%26adv_onsite%3D%26adv_icmp_dl%3D%26adv_channel%3DTyped%2FBookmarked%26adv_cmp_type%3Ddirect%26sc_dslv%3D; adv_values_session=adv_icmp%3Dop-mofu%3Adialog-mobilfunk%3Abnt%3Acredit%3Agigamobil-red-device-first__ab_mf_0095_web%3Aab%3Ap001%26adv_onsite%3D%26adv_icmp_dl%3D%26adv_channel%3DTyped%2FBookmarked%26adv_cmp_type%3Ddirect%26sc_dslv%3D; s_nr30=1662039695030-New; adv_flow_step=%7B%22step%22%3A1%2C%22flow%22%3A%22ols%20handys%3Avoice%3Abnt%22%2C%22flow_type_name%22%3A%22ols%20handys%3Avoice%3Abnt%22%7D; s_pers=%20s_lastvisit%3D1662039680598%7C1756647680598%3B%20s_nr%3D1662039695049-New%7C1664631695049%3B%20gpv_e27%3Dprivatkunden%253Amobilfunk%253Aangebote%2520mit%2520vertrag%253Ahandys%7C1662041495054%3B; adv_sft=ols%20handys%3Avoice%3Abnt; utag_main=v_id:0182f948dc700003570b0e92160905086001807e00978$_sn:1$_se:3$_ss:0$_st:1662041494312$ses_id:1662039678065%3Bexp-session$_pn:3%3Bexp-session$vapi_domain:vodafone.de$_prevpage:privatkunden%3Amobilfunk%3Aangebote%20mit%20vertrag%3Ahandys%3Bexp-1662043295060; s_sess=%20s_sq%3D%3B%20s_cc%3Dtrue%3B; mat_ep=https%3A//www.vodafone.de/privat/mobilfunk.html%3Ficmp%3Dhome%3Apathfinder-tile1%3Amobilfunk%2Chttps%3A//www.vodafone.de/privat/handys-tablets-tarife/alle-smartphones.html%3Ficmp%3Dop-mofu%3Adialog-mobilfunk%3Abnt%3Acredit%3Agigamobil-red-device-first__ab_mf_0095_web%3Aab%3Ap001; _uetsid=c3d054b029fb11ed878f19db683c7af5; _uetvid=5f0db820432f11eca81b67314fd06e31; kampyleSessionPageCounter=2; mbox=session#ceee91f39401427298e20b06a021466b#1662042359|PC#ceee91f39401427298e20b06a021466b.37_0#1725284496',
            'origin': 'https://www.vodafone.de',
            'pragma': 'no-cache',
            'referer': 'https://www.vodafone.de/privat/handys-tablets-tarife/alle-smartphones.html?icmp=op-mofu:dialog-mobilfunk:bnt:credit:gigamobil-red-device-first__ab_mf_0095_web:ab:p001',
            'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Microsoft Edge";v="104"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36 Edg/104.0.1293.70',
            'x-api-key': 'KSZZFe5MPWBLc3cLUPs2zW5at1w7xQyg'
        }
        self.headers_product = {
            'authority': 'www.vodafone.de',
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache',
            'content-type': 'application/json',
            'cookie': 'mdLogger=false; oshop=queryparams||vfchannel||1001; PV=97498552; at_check=true; s_ecid=MCMID%7C53216835864590585531455103526518212422; AMCVS_AE8901AC513145B60A490D4C%40AdobeOrg=1; adv_mf_group=disabled; adv_dc_rtg_cntrl=false; adv_et_entry=www.vodafone.de%2F; _gcl_au=1.1.834670519.1662039681; mat_tel=03eedbac-891a-4ca0-8102-989115488492; et_uk=8efcad2539814e04ae71ca320e7832da; et_gk=76ea79d7f3a849bcb470713a00c85f45|24.09.2022 13:23:00; _fbp=fb.1.1662039681958.809532218; AMCV_AE8901AC513145B60A490D4C%40AdobeOrg=-1124106680%7CMCMID%7C53216835864590585531455103526518212422%7CMCAID%7CNONE%7CMCOPTOUT-1662046882s%7CNONE%7CMCAAMLH-1662644482%7C6%7CMCAAMB-1662644482%7Cj8Odv6LonN4r3an7LhD3WZrU1bUpAkFkkiY1ncBR96t2PTI%7CMCCIDH%7C1623654709%7CvVersion%7C5.2.0%7CMCIDTS%7C19237; htLi=0800%20724%2026%2010; adv_qwt=4%7C0%7C0%7C4%7C0%7C0%7C0%7C0%7C0; kampyleUserSession=1662039684689; kampyleUserSessionsCount=5; kampyleUserPercentile=14.179048343490862; s_sess=%20s_sq%3D%3B%20s_cc%3Dtrue%3B; adv_session_pvc=5; s_nr30=1662041551273-New; adv_flow_step=%7B%22step%22%3A1%2C%22flow%22%3A%22ols%20handys%3Avoice%3Abnt%22%2C%22flow_type_name%22%3A%22ols%20product%20details%3Avoice%3Abnt%22%7D; adv_values=adv_icmp%3D%26adv_onsite%3D%26adv_icmp_dl%3D%26adv_channel%3DTyped%2FBookmarked%26adv_cmp_type%3Ddirect%26sc_dslv%3DLess%2Bthan%2B1%2Bday; adv_values_session=adv_icmp%3D%26adv_onsite%3D%26adv_icmp_dl%3D%26adv_channel%3DTyped%2FBookmarked%26adv_cmp_type%3Ddirect%26sc_dslv%3DLess%2Bthan%2B1%2Bday; s_pers=%20s_nr%3D1662041551299-New%7C1664633551299%3B%20s_lastvisit%3D1662041551303%7C1756649551303%3B%20gpv_e27%3Dprivatkunden%253Amobilfunk%253Aangebote%2520mit%2520vertrag%253Aproduktdetails%7C1662043351307%3B; adv_sft=ols%20product%20details%3Avoice%3Abnt; utag_main=v_id:0182f948dc700003570b0e92160905086001807e00978$_sn:1$_se:5$_ss:0$_st:1662043350433$ses_id:1662039678065%3Bexp-session$_pn:5%3Bexp-session$vapi_domain:vodafone.de$_prevpage:privatkunden%3Amobilfunk%3Aangebote%20mit%20vertrag%3Aproduktdetails%3Bexp-1662045151314; _uetsid=c3d054b029fb11ed878f19db683c7af5; _uetvid=5f0db820432f11eca81b67314fd06e31; kampyleSessionPageCounter=4; mat_ep=https%3A//www.vodafone.de/privat/handys-tablets-tarife/alle-smartphones.html%3Ficmp%3Dop-mofu%3Adialog-mobilfunk%3Abnt%3Acredit%3Agigamobil-red-device-first__ab_mf_0095_web%3Aab%3Ap001%2Chttps%3A//www.vodafone.de/privat/handys/samsung-galaxy-z-flip4.html; mbox=session#ceee91f39401427298e20b06a021466b#1662043446|PC#ceee91f39401427298e20b06a021466b.37_0#1725286386',
            'origin': 'https://www.vodafone.de',
            'pragma': 'no-cache',
            'referer': 'https://www.vodafone.de/privat/handys/samsung-galaxy-z-flip4.html',
            'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Microsoft Edge";v="104"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36 Edg/104.0.1293.70',
            'x-api-key': 'emz69zyRgQHkzFXmN6FG1QDchFqVboDD'
        }
        self.payload_data_tariff_consumer = {"offerBtx": "newContract", "offerSalesChannel": "Online.Consumer",
                                             "subscriptionIds": ["2501", "2502", "2503", "2505"],
                                             "discounts": []}
        self.payload_data_tariff_young = {"offerBtx": "newContract", "offerSalesChannel": "Online.Young",
                                          "subscriptionIds": ["2540", "2541", "2542", "2544"], "discounts": []
                                          }
        self.url_api_tariff = "https://www.vodafone.de/api/jarvis/v4/tariffOverview"  # tariff
        self.headers_tariff_consumer = {
            'authority': 'www.vodafone.de',
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'cookie': 'mdLogger=false; oshop=queryparams||vfchannel||1001; PV=97498552; at_check=true; s_ecid=MCMID%7C53216835864590585531455103526518212422; AMCVS_AE8901AC513145B60A490D4C%40AdobeOrg=1; adv_mf_group=disabled; adv_dc_rtg_cntrl=false; _gcl_au=1.1.834670519.1662039681; mat_tel=03eedbac-891a-4ca0-8102-989115488492; et_uk=8efcad2539814e04ae71ca320e7832da; et_gk=76ea79d7f3a849bcb470713a00c85f45|24.09.2022 13:23:00; _fbp=fb.1.1662039681958.809532218; AMCV_AE8901AC513145B60A490D4C%40AdobeOrg=-1124106680%7CMCMID%7C53216835864590585531455103526518212422%7CMCAID%7CNONE%7CMCOPTOUT-1662046882s%7CNONE%7CMCAAMLH-1662644482%7C6%7CMCAAMB-1662644482%7Cj8Odv6LonN4r3an7LhD3WZrU1bUpAkFkkiY1ncBR96t2PTI%7CMCCIDH%7C1623654709%7CvVersion%7C5.2.0%7CMCIDTS%7C19237; htLi=0800%20724%2026%2010; adv_qwt=4%7C0%7C0%7C4%7C0%7C0%7C0%7C0%7C0; kampyleUserSession=1662039684689; kampyleUserSessionsCount=5; kampyleUserPercentile=14.179048343490862; adv_sft=ols%20handys%3Avoice%3Abnt; adv_et_entry=www.vodafone.de%2Fprivat%2Fhandys%2Fsamsung-galaxy-z-flip4.html; adv_values=adv_icmp%3D%26adv_onsite%3D%26adv_icmp_dl%3D%26adv_channel%3DTyped%2FBookmarked%26adv_cmp_type%3Ddirect%26sc_dslv%3D; adv_values_session=adv_icmp%3D%26adv_onsite%3D%26adv_icmp_dl%3D%26adv_channel%3DTyped%2FBookmarked%26adv_cmp_type%3Ddirect%26sc_dslv%3D; adv_ret_category=younghwbnt; s_sess=%20s_sq%3D%3B%20s_cc%3Dtrue%3B; adv_session_pvc=12; s_nr30=1662044301114-Repeat; adv_flow_step=%7B%22step%22%3A6%2C%22flow%22%3A%22ols%20handys%3Avoice%3Abnt%22%2C%22flow_type_name%22%3A%22ols%20handys%3Avoice%3Abnt%22%7D; s_pers=%20s_lastvisit%3D1662044183313%7C1756652183313%3B%20s_nr%3D1662044301160-Repeat%7C1664636301160%3B%20gpv_e27%3Dprivatkunden%253Amobilfunk%253Aangebote%2520mit%2520vertrag%253Atarife%2520yolo%7C1662046101170%3B; utag_main=v_id:0182f948dc700003570b0e92160905086001807e00978$_sn:2$_se:6$_ss:0$_st:1662046097134$vapi_domain:vodafone.de$_prevpage:privatkunden%3Amobilfunk%3Aangebote%20mit%20vertrag%3Atarife%20yolo%3Bexp-1662047901184$ses_id:1662044180805%3Bexp-session$_pn:6%3Bexp-session; _uetsid=c3d054b029fb11ed878f19db683c7af5; _uetvid=5f0db820432f11eca81b67314fd06e31; mat_ep=https%3A//www.vodafone.de/privat/handys-tablets-tarife/alle-tarife-mit-vertrag.html%2Chttps%3A//www.vodafone.de/privat/handys-tablets-tarife/junge-leute.html; kampyleSessionPageCounter=11; mbox=PC#ceee91f39401427298e20b06a021466b.37_0#1725289317|session#bc0096a78ea24fdcbe150913f736e866#1662046377',
            'origin': 'https://www.vodafone.de',
            'referer': 'https://www.vodafone.de/privat/handys-tablets-tarife/alle-tarife-mit-vertrag.html',
            'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Microsoft Edge";v="104"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36 Edg/104.0.1293.70',
            'x-api-key': 'D1PzP0OmVu9IyS4R1gigvKRBOpuWAzlT'
        }
        self.headers_tariff_young = {
            'authority': 'www.vodafone.de',
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'cookie': 'mdLogger=false; oshop=queryparams||vfchannel||1001; PV=97498552; at_check=true; s_ecid=MCMID%7C53216835864590585531455103526518212422; AMCVS_AE8901AC513145B60A490D4C%40AdobeOrg=1; adv_mf_group=disabled; adv_dc_rtg_cntrl=false; _gcl_au=1.1.834670519.1662039681; mat_tel=03eedbac-891a-4ca0-8102-989115488492; et_uk=8efcad2539814e04ae71ca320e7832da; et_gk=76ea79d7f3a849bcb470713a00c85f45|24.09.2022 13:23:00; _fbp=fb.1.1662039681958.809532218; AMCV_AE8901AC513145B60A490D4C%40AdobeOrg=-1124106680%7CMCMID%7C53216835864590585531455103526518212422%7CMCAID%7CNONE%7CMCOPTOUT-1662046882s%7CNONE%7CMCAAMLH-1662644482%7C6%7CMCAAMB-1662644482%7Cj8Odv6LonN4r3an7LhD3WZrU1bUpAkFkkiY1ncBR96t2PTI%7CMCCIDH%7C1623654709%7CvVersion%7C5.2.0%7CMCIDTS%7C19237; htLi=0800%20724%2026%2010; adv_qwt=4%7C0%7C0%7C4%7C0%7C0%7C0%7C0%7C0; kampyleUserSession=1662039684689; kampyleUserSessionsCount=5; kampyleUserPercentile=14.179048343490862; adv_sft=ols%20handys%3Avoice%3Abnt; adv_et_entry=www.vodafone.de%2Fprivat%2Fhandys%2Fsamsung-galaxy-z-flip4.html; adv_values=adv_icmp%3D%26adv_onsite%3D%26adv_icmp_dl%3D%26adv_channel%3DTyped%2FBookmarked%26adv_cmp_type%3Ddirect%26sc_dslv%3D; adv_values_session=adv_icmp%3D%26adv_onsite%3D%26adv_icmp_dl%3D%26adv_channel%3DTyped%2FBookmarked%26adv_cmp_type%3Ddirect%26sc_dslv%3D; adv_ret_category=younghwbnt; s_sess=%20s_sq%3D%3B%20s_cc%3Dtrue%3B; mat_ep=https%3A//www.vodafone.de/privat/handys-tablets-tarife/junge-leute.html%2Chttps%3A//www.vodafone.de/privat/handys-tablets-tarife/alle-tarife-mit-vertrag.html; adv_session_pvc=11; s_nr30=1662044244519-Repeat; adv_flow_step=%7B%22step%22%3A5%2C%22flow%22%3A%22ols%20handys%3Avoice%3Abnt%22%2C%22flow_type_name%22%3A%22ols%20handys%3Avoice%3Abnt%22%7D; s_pers=%20s_lastvisit%3D1662044183313%7C1756652183313%3B%20s_nr%3D1662044244569-Repeat%7C1664636244569%3B%20gpv_e27%3Dprivatkunden%253Amobilfunk%253Aangebote%2520mit%2520vertrag%253Atarife%7C1662046044579%3B; utag_main=v_id:0182f948dc700003570b0e92160905086001807e00978$_sn:2$_se:5$_ss:0$_st:1662046039591$vapi_domain:vodafone.de$_prevpage:privatkunden%3Amobilfunk%3Aangebote%20mit%20vertrag%3Atarife%3Bexp-1662047844594$ses_id:1662044180805%3Bexp-session$_pn:5%3Bexp-session; _uetsid=c3d054b029fb11ed878f19db683c7af5; _uetvid=5f0db820432f11eca81b67314fd06e31; kampyleSessionPageCounter=10; mbox=PC#ceee91f39401427298e20b06a021466b.37_0#1725289097|session#bc0096a78ea24fdcbe150913f736e866#1662046157',
            'origin': 'https://www.vodafone.de',
            'referer': 'https://www.vodafone.de/privat/handys-tablets-tarife/junge-leute.html',
            'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Microsoft Edge";v="104"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36 Edg/104.0.1293.70',
            'x-api-key': 'D1PzP0OmVu9IyS4R1gigvKRBOpuWAzlT'
        }

    def main_process(self):
        self.crawl_category_page()
        # self.mobile_sdf.to_excel(r"C:\Users\7000027842\NO_SYNC\Ripos\test.xlsx", engine='xlsxwriter')
        self.upload_to_database()

    def crawl_category_page(self):
        i = 0
        while True:
            # print(i)
            time.sleep(10)
            response_category_page = requests.request("POST", self.url_api_category.format(str(i)),
                                                      headers=self.headers_category,
                                                      data=json.dumps(self.payload_data_category),
                                                      timeout=5000)
            if response_category_page.status_code == 200:
                json_category_page = json.loads(response_category_page.text)
                for json_cat_obj in json_category_page['devices']:
                    prod_deviceId = json_cat_obj['deviceId']
                    prod_atomicId = json_cat_obj['atomicId']
                    prod_model = json_cat_obj['name']
                    # print(prod_model)
                    prod_url = self.base_url + json_cat_obj['hubPageURL']
                    self.headers_product['referer'] = prod_url
                    self.payload_data_product["productIds"] = []
                    self.payload_data_product["productIds"].append(str(prod_deviceId))
                    time.sleep(10)
                    response_product_page = requests.request('POST', self.url_api_product,
                                                             headers=self.headers_product,
                                                             data=json.dumps(self.payload_data_product),
                                                             timeout=5000)
                    if response_product_page.status_code == 200:
                        json_product_page = json.loads(response_product_page.text)
                        prod_brand = json_product_page['vendor']

                        if self.check_brand(prod_brand):
                            for json_product_obj_color in json_product_page['colors']:
                                prod_color = json_product_obj_color['name']

                                for json_product_obj_capacity in json_product_obj_color['capacities']:
                                    prod_capacity = json_product_obj_capacity['size']
                                    prod_atomicId = json_product_obj_capacity['atomicId']
                                    prod_price = json_product_obj_capacity['totalPrice']['gross']
                                    self.payload_data_tariff_consumer["productIds"] = []
                                    self.payload_data_tariff_consumer["productIds"].append(str(prod_atomicId))
                                    self.payload_data_tariff_young["productIds"] = []
                                    self.payload_data_tariff_young["productIds"].append(str(prod_atomicId))
                                    dict_prod = {'stock': 'AVAILABLE', 'color': prod_color, 'model': prod_model,
                                                 'url': prod_url, 'brand': prod_brand,
                                                 'price': prod_price, 'capacity': prod_capacity,
                                                 'sku': prod_atomicId}
                                    time.sleep(10)
                                    response_tariffs_consumers = requests.request('POST', url=self.url_api_tariff,
                                                                                  headers=self.headers_tariff_consumer,
                                                                                  data=json.dumps(
                                                                                      self.payload_data_tariff_consumer),
                                                                                  timeout=5000)
                                    time.sleep(15)
                                    response_tariffs_young = requests.request('POST', url=self.url_api_tariff,
                                                                              headers=self.headers_tariff_young,
                                                                              data=json.dumps(
                                                                                  self.payload_data_tariff_young),
                                                                              timeout=5000)

                                    if response_tariffs_consumers.status_code == 200:
                                        json_tariffs_consumers = json.loads(response_tariffs_consumers.text)
                                        self.crawl_plans(json_tariffs_consumers, dict_prod)

                                    if response_tariffs_young.status_code == 200:
                                        json_tariffs_young = json.loads(response_tariffs_young.text)
                                        self.crawl_plans(json_tariffs_young, dict_prod)
            else:
                break

            i += 12

    def crawl_plans(self, json_tariff, dict_prod_plans):
        for json_plan_obj in json_tariff['subscription']:
            plan_id = json_plan_obj['subLevelId']
            cost_plan = 0
            cost_duration = 0
            if json_plan_obj['monetary'] is not False:
                plan_length = json_plan_obj['monetary'][-1]['runTimeEnd']
                for obj in json_plan_obj['monetary']:
                    cost_plan += obj['gross'] * (obj['runTimeEnd'] - obj['runTimeStart'] + 1)
                    cost_duration += (obj['runTimeEnd'] - obj['runTimeStart'] + 1)
                cost_plan_monthly = cost_plan / cost_duration
            else:
                plan_length = 24
                cost_plan_monthly = json_plan_obj['tariffPrice']['gross']

            if json_plan_obj['dataPush'] is False:
                plan_size = json_plan_obj['dataVolume']['amount']
                plan_name = json_plan_obj['label'] + ' with ' + json_plan_obj['dataVolume']['label']
            else:
                plan_size = json_plan_obj['dataPush'][0]['amount']
                plan_name = json_plan_obj['label'] + ' with ' + json_plan_obj['dataPush'][0]['label']
            cost_upfront = json_plan_obj['hardwareOneTimePrice']['gross']

            dict_prod_plans.update({'plan_wid': plan_id, 'plan_length': plan_length,
                                    'datagb': plan_size, 'plan_name': plan_name, 'upfront_cost': cost_upfront,
                                    'monthly_plan_cost': cost_plan_monthly, 'total_monthly_cost': cost_plan_monthly, })
            # print(dict_prod_plans)
            self.mobile_sdf = self.mobile_sdf.append(dict_prod_plans, ignore_index=True)


if __name__ == '__main__':
    scraper = VodafoneDeScraper()
    try:
        scraper.main_process()
    except Exception as e:
        scraper.send_error_db(err=str(e))
        raise SystemError
