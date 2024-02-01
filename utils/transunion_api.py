import datetime
import logging

from airflow.models import Variable


class TransUnionApi:
    def __init__(self, env: str):
        self.transport_username = Variable.get(f'transunion_{env}_transport_username')
        self.transport_password = Variable.get(f'transunion_{env}_transport_password')
        self.message_username = Variable.get(f'transunion_{env}_message_username')
        self.message_code = Variable.get(f'transunion_{env}_message_code')
        self.message_password = Variable.get(f'transunion_{env}_message_password')
        self.message_infinity_code = Variable.get(f'transunion_{env}_infinityCode')
        self.url = Variable.get(f'transunion_{env}_url')


    def get_soap_envelope(self, first_name, middle_name, surname, national_id, mobile_number, product_name):
        envelope = f'''
                <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ws="http://ws.crbws.transunion.ke.co/">
                   <soapenv:Header/>
                   <soapenv:Body>
                      <ws:get{product_name.capitalize()}>
                         <username>{self.message_username}</username>
                         <password>{self.message_password}</password>
                         <code>{self.message_code}</code>
                         <infinityCode>{self.message_infinity_code}</infinityCode>
                         <name1>{first_name if first_name is not None else ''}</name1>
                         <name2>{middle_name if middle_name is not None else ''}</name2>
                         <name3>{surname if surname is not None else ''}</name3>
                         <nationalID>{national_id}</nationalID>
                         <telephoneMobile>{mobile_number if mobile_number is not None else ''}</telephoneMobile>
                         <reportReason></reportReason>
                         <reportSector></reportSector>
                      </ws:get{product_name.capitalize()}>
                   </soapenv:Body>
                </soapenv:Envelope>
            '''.strip()

        if product_name == 'product139':
            envelope = envelope.replace('<reportReason></reportReason>', '<reportReason>4</reportReason>').replace(
                '<reportSector></reportSector>', '<reportSector>1</reportSector>')

        elif product_name == 'product103':
            envelope = envelope.replace('<reportReason></reportReason>', '<reportReason>3</reportReason>').replace(
                '<reportSector></reportSector>', '')

        return envelope


    def get_product(self, national_id, first_name, middle_name, surname, product_name, mobile_number=None):
        import requests
        if product_name not in ('product139', 'product103'):
            raise ValueError("Invalid product name. Allowed values are 'product139' or 'product103'.")

        # Define the SOAP envelope
        soap_envelope = self.get_soap_envelope(
            national_id=national_id,
            first_name=first_name,
            surname=surname,
            middle_name=middle_name,
            mobile_number=mobile_number,
            product_name=product_name
        )

        # Send the SOAP request
        response = requests.post(
            url=self.url,
            data=soap_envelope,
            headers={'Content-Type': 'text/xml; charset=utf-8'},
            auth=requests.auth.HTTPBasicAuth(self.transport_username, self.transport_password)
        )
        if response.status_code != 200:
            logging.warning(f"------RAW REQUEST ------- \n {soap_envelope} \n ------------------")
            response.raise_for_status()

        return {
            'timestamp': datetime.datetime.utcnow(),
            'national_id': national_id,
            'raw_request': soap_envelope,
            'raw_response': response.content.decode('utf-8')
        }
