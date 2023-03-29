import sqlite3
from telegram import Update, MessageEntity
from telegram.ext import Updater, Job, JobQueue, CallbackContext, CommandHandler, ConversationHandler, Filters, MessageHandler
from sys import argv
import datetime
from urllib.parse import urlparse
import pytz
from pytz import timezone
import json

db = sqlite3.connect('data.db', check_same_thread=False)
shuffle_count = 0
marten_n = 0
marten_count = 0

if 'sql' in argv:
    def execute_inp(cursor, text):
        try:
            cursor.execute(text)
            rows = cursor.fetchall()
            table = tabulate([tuple(r) for r in rows], headers=rows[0].keys()) if len(rows) > 0 else ''
            print('%s (%s):\n%s' % ('Rows Modified' if cursor.rowcount != -1 else 'Rows', len(rows) or cursor.rowcount, table))
        except sqlite3.Error as e:
            print('[SQL Error]\n' + str(e))
    from tabulate import tabulate
    db.row_factory = sqlite3.Row
    c = db.cursor()
    last_file_path = None
    while True:
        inp = input('>>> ')
        if inp[0] == '\\':
            if inp[1] == 'q': #quit
                quit()
            elif inp[1] == 'c': # commit
                db.commit()
            elif inp[1] == 'r': # rollback
                db.rollback()
            elif inp[1] == 'f': # file
                s = inp.split(' ')
                path = None
                if len(s) != 2:
                    if not last_file_path:
                        print('Error: Expected file path.')
                        continue
                    else:
                        path = last_file_path
                else:
                    path = s[1]
                try:
                    with open(path) as file:
                        last_file_path = path
                        execute_inp(c, file.read())
                except FileNotFoundError:
                    last_file_path = None
                    print('Error: File not found.')
                    continue
            elif inp[1] in [ '?', 'h' ]:
                print('Commands:\n%s' % '\n'.join([
                    '\\quit: Quit without commiting any changes to the database.',
                    '\\commit: Commit all changes to the database.',
                    '\\help: Show this message.',
                    '\\rollback: Revert any changes since the last commit.'
                ]))
        else:
            execute_inp(c, inp)
    quit()

if 'initdb' in argv:
    c = db.cursor()
    c.execute("SELECT count(rowid) FROM sqlite_master WHERE type='table' AND name='martens'")
    if len(c.fetchone()[0]) == 0:
        c.execute("CREATE TABLE martens (link TEXT, link_title TEXT, image TEXT, views INT, shuffle INT)")
        print('Created table "martens" in database.')
    else:
        print('Table "martens" already exists, skipping.')
    c.execute("SELECT count(rowid) FROM sqlite_master WHERE type='table' AND name='foxes'")
    if len(c.fetchone()[0]) == 0:
        c.execute("CREATE TABLE foxes (link TEXT, link_title TEXT, image TEXT, views INT, shuffle INT)")
        print('Created table "foxes" in database.')
    else:
        print('Table "foxes" already exists, skipping.')
    db.commit()

def get_link_title_from_url(url: str):
    return '.'.join(urlparse(url).netloc.split('.')[-2:])

class MartenBlob(object):
    def __init__(self, _id, link, link_title, image, views, shuffle):
        self.id = _id
        self.link = link
        self.link_title = link_title or ('from %s' % get_link_title_from_url(link))
        self.image = image
        self.views = views
        self.shuffle = shuffle
    def send(self, chat_id, bot):
        return bot.send_photo(chat_id, photo=self.image, caption=self.link_title, caption_entities=[MessageEntity(type='text_link', url=self.link, offset=0, length=len(self.link_title))])

def get_random_marten():
    global shuffle_count, marten_n, marten_count
    c = db.cursor()
    c.execute("SELECT rowid, link, link_title, image, views, shuffle FROM martens WHERE shuffle=? ORDER BY RANDOM() LIMIT 1", (shuffle_count, ))
    row = c.fetchone()
    c.execute("UPDATE martens SET shuffle=? WHERE rowid=?", (shuffle_count + 1, row[0], ))
    db.commit()
    marten_n += 1
    if marten_n == marten_count:
        marten_n = 0
        shuffle_count += 1
        get_marten_count(shuffle_count)
    return MartenBlob(row[0], row[1], row[2], row[3], row[4], row[5])

def get_marten_count(shuffle_n: int):
    c = db.cursor()
    c.execute("SELECT count(rowid) AS count FROM martens WHERE shuffle=?", (shuffle_n, ))
    return c.fetchone()[0]

def daily(context):
    print('Posting a marten...')
    get_random_marten().send(context.bot_data['channel_id'], context.bot)

def remove_job_if_exists(name, context):
    """Remove job with given name. Returns whether job was removed."""
    current_jobs = context.job_queue.get_jobs_by_name(name)
    if not current_jobs:
        return False
    for job in current_jobs:
        job.schedule_removal()
    return True

def start(update: Update, context: CallbackContext):
    """Start sending daily martens."""
    if update.effective_user.username != 'VirtualMarten':
        context.bot.send_message(update.effective_chat.id, text="Not authorized.")
        return
    context.bot.send_message(update.effective_chat.id, text="Running.")
    old_job = remove_job_if_exists('daily', context)
    if old_job:
        print("Removed old daily job.")
    context.job_queue.run_daily(daily, datetime.time(8, 0, 0, 0, timezone('US/Eastern')), context=int(context.bot_data['channel_id']), name='daily')
    #context.job_queue.run_repeating(daily, 60, name='daily')
    print('Scheduled daily job.')

def send(update: Update, context: CallbackContext):
    """Send a text marten in the Marten Daily channel to make sure the bot is working."""
    if update.effective_user.username != 'VirtualMarten':
        context.bot.send_message(update.effective_chat.id, text="Not authorized.")
        return
    context.bot.send_message(update.effective_chat.id, text="Sending a random marten to channel...")
    get_random_marten().send(context.bot_data['channel_id'], context.bot)

def stop(update: Update, context: CallbackContext):
    """Stop sending daily martens."""
    if update.effective_user.username != 'VirtualMarten':
        context.bot.send_message(update.effective_chat.id, text="Not authorized.")
        return
    context.bot.send_message(update.effective_chat.id, text="Stopped.")
    job = remove_job_if_exists('daily', context)
    if job:
        print('Removed daily job.')

def list_get(list, i):
    try:
        return list[i]
    except IndexError:
        return None

def add_marten(update: Update, context: CallbackContext):
    """Add a new marten. /add <url> [url_title]"""
    global shuffle_count
    if update.effective_user.username != 'VirtualMarten':
        context.bot.send_message(update.effective_chat.id, text="Not authorized.")
        return ConversationHandler.END
    if not (len(context.args) >= 1):
        context.bot.send_message(update.effective_chat.id, text="Too few arguments.\nUsage: /add <url> [url_title]")
        return ConversationHandler.END
    # Check to see if an entry already uses that link, to help avoid duplicates
    c = db.cursor()
    c.execute("SELECT count(rowid) FROM martens WHERE link=?", (context.args[0],))
    if c.fetchone()[0] > 0:
        context.bot.send_message(update.effective_chat.id, text="There is already a marten with that link.")
        return ConversationHandler.END
    context.bot.send_message(update.effective_chat.id, text="Send the image you'd like to attach.")
    context.dispatcher.bot_data['link'] = context.args[0]
    if len(context.args) > 1:
        context.dispatcher.bot_data['link_title'] = ' '.join(context.args[1:])
    else:
        context.dispatcher.bot_data['link_title'] = ""
    return 1

def add_photo(update: Update, context: CallbackContext):
    global marten_count
    if len(update.message.photo):
        image = update.message.photo[-1].file_id
    else:
        image = update.message.text
    c = db.cursor()
    try:
        c.execute("INSERT INTO martens (link, link_title, image, views, shuffle) VALUES (?, ?, ?, 0, ?)", (context.dispatcher.bot_data['link'], context.dispatcher.bot_data['link_title'], image, shuffle_count, ))
    except Exception as e:
        context.bot.send_message(update.effective_chat.id, text="Database error. See log for details.")
        db.rollback()
        print(e)
        return ConversationHandler.END
    db.commit()
    marten_count += 1
    context.bot.send_message(update.effective_chat.id, text="Added new marten to database.")
    del context.dispatcher.bot_data['link']
    del context.dispatcher.bot_data['link_title']
    return ConversationHandler.END

def cancel_add(update: Update, context: CallbackContext):
    context.bot.send_message(update.effective_chat.id, text="Canceled.")
    del context.dispatcher.bot_data['link']
    del context.dispatcher.bot_data['link_title']
    return ConversationHandler.END

def sstat(update: Update, context: CallbackContext):
    global marten_count, marten_n, shuffle_count
    c = db.cursor()
    c.execute("SELECT count(rowid) FROM martens")
    rmc = c.fetchone()[0]
    c.execute("SELECT count(rowid) FROM martens WHERE shuffle=?", (shuffle_count, ))
    current_shuffle_count = c.fetchone()[0]
    c.execute("SELECT count(rowid) FROM martens WHERE shuffle=?", (shuffle_count + 1, ))
    next_shuffle_count = c.fetchone()[0]
    context.bot.send_message(update.effective_chat.id, text=f"Marten Count: {marten_count} (Real {rmc})\nMarten N: {marten_n}\nShuffle Count: {shuffle_count}\nCurrent Shuffle Count: {current_shuffle_count}\nNext Shuffle Count: {next_shuffle_count}")

def marten_list(update: Update, context: CallbackContext):
    c = db.cursor()
    c.execute("SELECT link FROM martens")
    rows = c.fetchall()
    s = []
    for r in rows:
        s.append(r[0])
    s = '\n'.join(s)
    with open('list.txt', 'w+') as file:
        file.write(s)
        file.seek(0, 0)
        context.bot.send_document(update.effective_chat.id, document=file, filename='list.txt')

def _load():
    global marten_n, shuffle_count
    try:
        with open('data.json', 'r') as file:
            o = json.load(file)
            marten_n = o['marten']
            shuffle_count = o['shuffle']
            return True
    except FileNotFoundError:
        return False

def _save():
    global marten_n, shuffle_count
    print('Saving shuffle information to "data.json".')
    with open('data.json', 'w') as file:
        json.dump({ 'shuffle': shuffle_count, 'marten': marten_n }, file)

def save(update: Update, context: CallbackContext):
    _save()

def main():
    global marten_count

    token = None
    with open('token') as file:
        token = file.read(46)

    if not token:
        print('Missing bot token.')
        quit()

    updater = Updater(token)

    channel = updater.bot.get_chat("@marten_daily")
    dispatcher = updater.dispatcher
    dispatcher.bot_data['channel_id'] = channel.id

    dispatcher.add_handler(CommandHandler('start', start))
    dispatcher.add_handler(CommandHandler('stop', stop))
    dispatcher.add_handler(CommandHandler('send', send))
    dispatcher.add_handler(CommandHandler('save', save))
    dispatcher.add_handler(CommandHandler('sstat', sstat))
    dispatcher.add_handler(CommandHandler('list', marten_list))
    dispatcher.add_handler(CommandHandler('marten_list', marten_list))

    dispatcher.add_handler(ConversationHandler(
        entry_points=[ CommandHandler('add', add_marten), CommandHandler('add_marten', add_marten) ],
        states={
            1: [ MessageHandler(Filters.photo, add_photo) ]
        },
        fallbacks=[ CommandHandler('cancel', cancel_add) ]
    ))

    if not _load():
        print('Could not load shuffle information from "data.json", resetting the shuffle.')
        c = db.cursor()
        c.execute("UPDATE martens SET shuffle=0")
        db.commit()
    marten_count = get_marten_count(0)

    updater.start_polling()
    updater.idle()

    _save()

if __name__ == '__main__':
    main()