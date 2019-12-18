#!/usr/bin/env python

"""Chiron support for Hangouts"""

import argparse
import asyncio
import logging
import os
import sys
import threading
import traceback

from collections import OrderedDict
from six.moves import configparser

from typing import Any, Callable, Dict, Optional

import hangups
import zulip

logger = logging.getLogger(__name__)

class Bridge_ConfigException(Exception):
    pass

class Bridge_ZulipFatalException(Exception):
    pass


def format_user(user):
    # type: (hangups.User) -> str
    """Format a Hangouts User object for display"""
    return "%s %s" % (user.full_name, user.emails)

HangoutsCallbackType = Callable[[hangups.ConversationList, hangups.ConversationEvent], None]

class Bridge:
    "Zulip/Hangouts bridge"""

    def __init__(self, hangouts_client: hangups.Client,
                 zulip_client: zulip.Client, zulip_config: Dict[str, str],
                 streams: Dict[str, str], event_loop: asyncio.AbstractEventLoop) -> None:
        self._hangouts_client = hangouts_client
        self.hangouts_conv_list = None
        self._zulip_client = zulip_client
        self._streams = streams
        self._default_stream = zulip_config.get('default_stream')
        self._default_topic = zulip_config.get('default_topic', '(no topic)')
        self._event_loop = event_loop

    def send_hangout(self, conv_id, segments):
        # type: (Any, str, str) -> bool
        if not self.hangouts_conv_list:
            logger.warning("no conversation list yet -- please wait...")
            return False
        try:
            conversation = self.hangouts_conv_list.get(conv_id)
        except KeyError:
            logger.warning("conv_id %s not found to send %s", conv_id, segments)
            return False
        self._event_loop.create_task(conversation.send_message(segments))
        return True

    def build_zulip_processor(self):
        # type: (Any) -> Callable[[Dict[str,Any]], None]
        def zulip_to_hangouts(msg):
            # type: (Dict[str,Any]) -> None
            """Zulip -> Hangouts"""
            if msg['type'] != 'stream':
                # ignore personals
                return
            sender = msg['sender_full_name']
            stream = msg['display_recipient']
            logger.info("got message: %s", msg)
            if msg['sender_short_name'].endswith('-bot'):
                logger.info("Ignoring message on stream %s from bot %s (%s)",
                            stream, sender, msg['sender_short_name'])
                return
            hangout = self._streams.get(stream, None)
            if not hangout:
                logger.warning("Message received on unknown stream %s from %s",
                               stream, msg['sender_email'])
                return
            if msg['subject'] == self._default_topic:
                subject = ""
            else:
                subject = "[%s] " % (msg['subject'],)
            segments = [hangups.ChatMessageSegment(sender, is_bold=True, is_italic=True)]
            segments.append(hangups.ChatMessageSegment(": "+subject))
            segments.extend(hangups.ChatMessageSegment.from_str(msg['content']))
            self.send_hangout(hangout, segments)
        return zulip_to_hangouts

    def get_stream_from_hangouts_event(self, conv_event):
        # type: (Any, hangups.ConversationEvent) -> str
        for stream, hangout in self._streams.items():
            if conv_event.conversation_id == hangout:
                return stream

        # Finished loop, so we don't know this hangout
        conversation = self.hangouts_conv_list.get(conv_event.conversation_id)
        conv_name = conversation.name
        logger.warning("unknown hangout: %s %s", conv_event.conversation_id, conv_name)
        return self._default_stream

    def hangouts_to_zulip(self, conv_list, conv_event):
        # type: (hangups.ConversationList, hangups.ConversationEvent) -> None
        """Hangouts -> Zulip"""
        logger.info("got hangouts message")
        self.hangouts_conv_list = conv_list
        if isinstance(conv_event, hangups.ChatMessageEvent):
            sender_user = conv_list._user_list.get_user(conv_event.user_id)
            if sender_user.is_self:
                logger.info("Ignoring message from self")
                return
            sender = format_user(sender_user)
            stream = self.get_stream_from_hangouts_event(conv_event)
            content = "***%s***: %s" % (sender_user.full_name, conv_event.text)

            reply_data = {
                "type": "stream",
                "to": stream,
                "subject": self._default_topic,
                "content": content,
            }
            result = self._zulip_client.send_message(reply_data)
            if result['result'] != 'success':
                logger.warning("tried to send zulip %s, got result %s", reply_data, result)

    def run_hangouts(self):
        # type: (Any) -> None
        loop = asyncio.get_event_loop()
        task = asyncio.ensure_future(_async_main(self, self._hangouts_client),
                                     loop=loop)
        try:
            print("Listening...")
            loop.run_until_complete(task)
            print("returning from run_hangouts...")
        except KeyboardInterrupt:
            task.cancel()
            loop.run_until_complete(task)
        finally:
            loop.close()


async def _async_main(bridge: Bridge, client: hangups.Client) -> None:
    """Run the example coroutine."""
    logger.info("in async_main...")
    # Spawn a task for hangups to run in parallel with the handler coroutine.
    task = asyncio.ensure_future(client.connect())

    # Wait for hangups to either finish connecting or raise an exception.
    on_connect = asyncio.Future()  # type: ignore
    client.on_connect.add_observer(lambda: on_connect.set_result(None))
    done, _ = await asyncio.wait(
        (on_connect, task), return_when=asyncio.FIRST_COMPLETED
    )
    await asyncio.gather(*done)

    # Run the handler coroutine. Afterwards, disconnect hangups gracefully and
    # yield the hangups task to handle any exceptions.
    try:
        await receive_messages(bridge, client)
    except asyncio.CancelledError:
        pass
    finally:
        await client.disconnect()
        await task


async def receive_messages(bridge: Bridge, client: hangups.Client) -> None:
    """Setup observer to handle messages"""
    print('loading conversation list...')
    dummy_user_list, conv_list = (
        await hangups.build_user_conversation_list(client)
    )
    bridge.hangouts_conv_list = conv_list
    event_cb = lambda event: bridge.hangouts_to_zulip(conv_list, event)
    conv_list.on_event.add_observer(event_cb)

    print('waiting for chat messages...')
    while True:
        await asyncio.sleep(1)


def generate_parser():
    # type: () -> argparse.ArgumentParser
    description = """
    Script to bridge between Zulip and Hangouts.
    """

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-c', '--config', required=False,
                        help="Path to the config file for the bridge.")
    parser.add_argument('--write-sample-config', metavar='PATH', dest='sample_config',
                        help="Generate a configuration template at the specified location.")
    parser.add_argument('--from-zuliprc', metavar='ZULIPRC', dest='zuliprc',
                        help="Optional path to zuliprc file for bot, when using --write-sample-config")
    return parser


def read_configuration(config_file):
    # type: (str) -> Dict[str, Dict[str, str]]
    config = configparser.ConfigParser()

    try:
        config.read(config_file)
    except configparser.Error as exception:
        raise Bridge_ConfigException(str(exception))

    if set(config.sections()) != {'hangouts', 'zulip', 'streams'}:
        raise Bridge_ConfigException("Please ensure the configuration has hangouts, zulip, and streams sections.")

    # TODO Could add more checks for configuration content here

    return {section: dict(config[section]) for section in config.sections()}


def write_sample_config(target_path, zuliprc):
    # type: (str, Optional[str]) -> None
    if os.path.exists(target_path):
        raise Bridge_ConfigException("Path '{}' exists; not overwriting existing file.".format(target_path))

    sample_dict = OrderedDict((
        ('hangouts', OrderedDict((
            ('token', '~/.config/hangups/hangouts.token'),
        ))),
        ('zulip', OrderedDict((
            ('email', 'hangouts-bot@chat.zulip.org'),
            ('api_key', 'aPiKeY'),
            ('site', 'https://chat.zulip.org'),
        ))),
        ('streams', OrderedDict((
            ('test here', 'hangoutid'),
        ))),
    ))

    if zuliprc is not None:
        if not os.path.exists(zuliprc):
            raise Bridge_ConfigException("Zuliprc file '{}' does not exist.".format(zuliprc))

        zuliprc_config = configparser.ConfigParser()
        try:
            zuliprc_config.read(zuliprc)
        except configparser.Error as exception:
            raise Bridge_ConfigException(str(exception))

        # Can add more checks for validity of zuliprc file here

        sample_dict['zulip']['email'] = zuliprc_config['api']['email']
        sample_dict['zulip']['site'] = zuliprc_config['api']['site']
        sample_dict['zulip']['api_key'] = zuliprc_config['api']['key']

    sample = configparser.ConfigParser()
    sample.read_dict(sample_dict)
    with open(target_path, 'w') as target:
        sample.write(target)


def main():
    # type: () -> None
    # signal.signal(signal.SIGINT, die)
    logging.basicConfig(level=logging.INFO)

    parser = generate_parser()
    options = parser.parse_args()

    if options.sample_config:
        try:
            write_sample_config(options.sample_config, options.zuliprc)
        except Bridge_ConfigException as exception:
            print("Could not write sample config: {}".format(exception))
            sys.exit(1)
        if options.zuliprc is None:
            print("Wrote sample configuration to '{}'".format(options.sample_config))
        else:
            print("Wrote sample configuration to '{}' using zuliprc file '{}'"
                  .format(options.sample_config, options.zuliprc))
        sys.exit(0)
    elif not options.config:
        print("Options required: -c or --config to run, OR --write-sample-config.")
        parser.print_usage()
        sys.exit(1)

    try:
        config = read_configuration(options.config)
    except Bridge_ConfigException as exception:
        print("Could not parse config file: {}".format(exception))
        sys.exit(1)

    # Get config for each client
    hangouts_config = config["hangouts"]
    zulip_config = config["zulip"]
    stream_config = config["streams"]

    # Initiate clients
    backoff = zulip.RandomExponentialBackoff(timeout_success_equivalent=300)
    while backoff.keep_going():
        print("Starting hangouts mirroring bot")
        try:
            cookies = hangups.auth.get_auth_stdin(hangouts_config['token'])
            hangouts_client = hangups.Client(cookies)

            zulip_client = zulip.Client(email=zulip_config["email"],
                                        api_key=zulip_config["api_key"],
                                        site=zulip_config["site"])

            loop = asyncio.get_event_loop()
            bridge = Bridge(hangouts_client, zulip_client, zulip_config, stream_config, loop)

            print("Starting message handler on Zulip client")
            zulip_func = lambda: zulip_client.call_on_each_message(bridge.build_zulip_processor())
            zulip_thread = threading.Thread(target=zulip_func)
            zulip_thread.start()

            print("Starting message handler on Hangouts client and waiting")
            bridge.run_hangouts()

        except Bridge_ZulipFatalException as exception:
            sys.exit("Zulip bridge error: {}".format(exception))
        except zulip.ZulipError as exception:
            sys.exit("Zulip error: {}".format(exception))
        except Exception as e:
            traceback.print_exc()
        backoff.fail()

if __name__ == '__main__':
    main()
