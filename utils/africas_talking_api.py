import africastalking
from airflow.models import Variable

class SMS:
    def __init__(self):
        """
        Initialize the SMS object for sending messages using the Africa's Talking API.

        The credentials (username and API key) are fetched from the Airflow Variable named 'africas_talking_asante2'.
        """

        self.username = "asante2"
        self.api_key = Variable.get('africas_talking_asante2')

        # Initialize the SDK
        africastalking.initialize(self.username, self.api_key)

        # Get the SMS service
        self.sms = africastalking.SMS

    def send(self, recipients: list, message: str):
        """
        Send an SMS to one or more recipients using the Africa's Talking API.

        :param recipients: A list of phone numbers (in international format) to send the SMS to.
        :type recipients: List[str]

        :param message: The text message to be sent.
        :type message: str

        :return: A dictionary containing the API response for the SMS send request
        :rtype: dict
        """

        # Set the numbers you want to send to in international format

        # Set your shortCode or senderId
        sender = "ASANTE_FSL"

        return self.sms.send(
            message=message, recipients=recipients,
            sender_id=sender, enqueue=False, callback=None
        )
