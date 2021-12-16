import json
import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pygtail import Pygtail

tp_executor = ThreadPoolExecutor(3)

class EventTimedStorage:
    def __init__(self, duration):
          self.duration = datetime.timedelta(minutes=duration)
          self.events = []
          self.shops = {'shops.computeruniverse' : True}
          self.not_shops = ['worker', 'discord_webhook.webhook']

    def add_event(self, timestamp, type, event, shop=None):
        self.events.append((timestamp, type, shop, event))
        self.events = list(filter(lambda ts_ev: ts_ev[0] + self.duration >= timestamp, self.events))
        if shop != None and shop not in self.shops and shop not in self.not_shops:
            self.shops[shop] = True

    def _get_events(self, timestamp):
        self.events = list(filter(lambda ts_ev: ts_ev[0] + self.duration >= timestamp, self.events))
        return self.events
    
    def _get_last_shops(self, timestamp):
        self.events = list(filter(lambda ts_ev: ts_ev[0] + self.duration >= timestamp, self.events))
        return [e[2] for e in self.events]


class NotifierEventStorage(EventTimedStorage):
    def __init__(self, duration):
        EventTimedStorage.__init__(self, duration)
        self.is_broke_param = {
            False : 'Notifier is work',
            True : 'Notifier is broken',
        }

    def is_notifier_broken(self, timestmp):
        return sum(e[1] == 'ERROR' for e in self._get_events(timestmp)) > 1


class MonitorEventStorage(EventTimedStorage):
    def __init__(self, duration):
        EventTimedStorage.__init__(self, duration)
        self.is_broke_param = {
            True : 'Monitor is work',
            False : 'Monitor is broken',
        }
        self.message_param = {
            True : 'Monitor restarted',
            False : 'Cannot give items last 15 minute',
        }

    # def is_monitor_broken(self, timestmp):
    #     return sum(e[1] == 'ERROR' for e in self._get_events(timestmp)) > 1
    
    def is_monitor_broken(self, timestmp):
        result = {}
        shop = None
        last_shops = self._get_last_shops(timestmp)
        if last_shops == []:
            return {}
        for s in self.shops:
            if s not in last_shops:
                result[s] = False
                shop = s
            else:
                result[s] = True
        return result


class LogCheck():
    def __init__(self):
        self.path = None
        self.webhook = None
        self.timestamp_last_restart = None
        self.sleep_time = datetime.timedelta(minutes=15).total_seconds()

    def notify(self, log):
        log['webhooks'] = self.webhook
        self.send_notification(**log)
        print(log)
    
    def restart_docker(self, shop):
        try:
            if (self.timestamp_last_restart == None 
                or (datetime.datetime.now() - self.timestamp_last_restart).total_seconds() > self.sleep_time
                or shop == 'shops.computeruniverse'):
                # open("/to_check/logs/restart_state.txt", "w")
                open("Production/MessiahNotify/logs/restart_state.txt", "w")
                self.timestamp_last_restart = datetime.datetime.now()
        except Exception as e:
            raise e

    def send_notification_impl(self, info, asctime, name, webhooks, levelname, 
                                message, override=None, **kwargs):
        if not override:
            override = {}       

        webhook = DiscordWebhook(url=webhooks)
        embed = DiscordEmbed(title='event: ' + info + ', time: ' + asctime + ' ' + ', file name: ' + name,
                            description=message,
                            color=override.get('color', 0),
                            )
        webhook.add_embed(embed)
        webhook.execute()

    def send_notification(self, **kwargs):
        self.send_notification_impl(**kwargs)

    def return_log(self):
        tail = Pygtail(self.path, paranoid=True, offset_file='log.offset')
        try:
            line = tail.next()
            log = json.loads(line)
            return log
        except:
            tail._update_offset_file()
        

    def check_on_error(self):
        pass


class LogCheckNotify(LogCheck):
    def __init__(self):
        LogCheck.__init__(self)
        self.path = 'daemon.log'
        self.webhook = 'https://discord.com/api/webhooks/871656292568162334/iOwyRZMwH-rDE_dkVPpsmaUXeHPxmmGT9RaxnqiSMVrzN2tM1vDDdT2WCBe7ZdrY5Bdt'
        self.is_broke = 0

    def check_on_error(self):
        notifier_checker = NotifierEventStorage(15)
        while not is_stopped():
            log = self.return_log()
            if log != None:
                try:
                    timestamp = datetime.datetime.strptime(log['asctime'], '%Y-%m-%d %H:%M:%S,%f')
                    notifier_checker.add_event(timestamp, log['levelname'], log['message'])
                    current_time = datetime.datetime.now()
                    is_broke = notifier_checker.is_notifier_broken(current_time)
                    if is_broke != self.is_broke:
                        self.is_broke = is_broke
                        log['info'] = notifier_checker.is_broke_param[self.is_broke]
                        self.notify(log)
                except:
                    raise


class LogCheckMonitors(LogCheck):
    def __init__(self):
        LogCheck.__init__(self)
        self.path = 'daemon.log'
        self.webhook = 'https://discord.com/api/webhooks/871656292568162334/iOwyRZMwH-rDE_dkVPpsmaUXeHPxmmGT9RaxnqiSMVrzN2tM1vDDdT2WCBe7ZdrY5Bdt'
        self.shops_state = {}

    def check_on_error(self):
        checker = MonitorEventStorage(15)
        while not is_stopped():
            log = self.return_log()
            if log != None:
                try:
                    timestamp = datetime.datetime.strptime(log['asctime'], '%Y-%m-%d %H:%M:%S,%f')
                    checker.add_event(timestamp, log['levelname'], log['message'], log['name'])
                    self.shops_state = checker.shops
                    current_time = datetime.datetime.now()
                    is_state_change= checker.is_monitor_broken(current_time)
                    if is_state_change != {} and is_state_change != self.shops_state:
                        broken_shop = [s for s in is_state_change if is_state_change[s] != self.shops_state[s]]
                        checker.shops = is_state_change
                        for shop in broken_shop:
                            log['name'] = shop
                            log['levelname'] = 'INFO'
                            log['info'] = checker.is_broke_param[is_state_change[shop]]
                            log['message'] = checker.message_param[is_state_change[shop]]
                            # self.notify(log)
                            if is_state_change[shop] == False:
                                self.restart_docker(shop)
                except:
                    raise


checker_stopped = [False]

checkers = [
    # LogCheckNotify,
    LogCheckMonitors,
]

pp_executor = ProcessPoolExecutor(3)


def is_stopped():
    return checker_stopped[0]


def start_checker():
    # apis = [api_cls() for api_cls in checkers]

    # futures = [(pp_executor.submit(api.check_on_error))
    #            for api in apis]
    # return futures
    LogCheckMonitors().check_on_error()

def stop_checker():
    print('Stopping checker')
    global checker_stopped
    checker_stopped[0] = True    