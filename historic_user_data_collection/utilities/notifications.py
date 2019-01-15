import sys

"""
Base class intended for implementing notifications on various platforms.
Override the `__init__` method with any static configuration and override
`notify` to send a message.

Abstract base class, designed to be extended.
"""
class Notification:

    def notify(self, message):
        raise NotImplementedError("In order to send a notification, please "
        + "implement notify in the subclass")

    def get_flag(self, notify_type):
        flags = {
            "start": "⏳",
            "complete": "🔔",
            "warning": "🚨",
            "error": "🙀"
        }

        return flags.get(notify_type, "💬")

"""
Simple notifier that prints messages to the console.
"""
class TestNotification(Notification):

    def notify(self, message, notify_type="info"):
        flag = self.get_flag(notify_type)

        print("{flag} {message}"
            .format(flag=flag, message=message), flush=True)



def get_notifiers(config):
    notifier_js = config["notifier_params"]
    notifiers = []
    for notifier_type, notifier_params in notifier_js:
        notifiers.append(getattr(sys.modules[__name__], notifier_type))
    return notifiers

def notify_all(notifiers, message, notify_type="info"):
    for notifier in notifiers:
        notifier.notify(message, notify_type)

# """
# Slack notifications.
# """
# class SlackNotification:
#
#     def __init__(self, channel, bot_token):
#         self.channel = channel
#         self.bot_token = bot_token
#
#
#     def notify(self, message):
#         from slackclient import SlackClient
#         slack_client = SlackClient(self.bot_token)
#
#         slack_client.api_call(
#             "chat.postEphemeral",
#             channel="#leos_test_channel",
#             text="🚨 INCOMING NOTIFICATION 🚨 {}".format(message),
#         )
#
# bot_token = "xoxb-6008658867-505420171330-geJNN0CIJCNJVZXk2YuuQqlR"
# n = SlackNotification(None)
# n.notify("beep boop")
