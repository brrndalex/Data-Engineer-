# Импортируем библиотеки

import logging
import telebot
from googletrans import Translator
from telebot import types

# добавим логгирование, чтобы получать сообщения в консоль

logger = telebot.logger
telebot.logger.setLevel(logging.DEBUG)

# Создаем переводчик
translator = Translator()

# словарь для хранения выбранного языка
selectLang = {}

# токен телеграм-бота
token = "6259230595:AAFEZo0v_V6U4VBOsS5yylH22e219WgvEfM"

# создаем бота
bot = telebot.TeleBot(token, parse_mode=None)


# обработчик команды start
@bot.message_handler(commands=['start'])
def send_welcome(message):
    """ Respond to a /start message. """
    bot.reply_to(message, """\
        Привет! Я бот-переводчик. Я могу помочь тебе(вам) перевести текст:\n
        - с Русского: RU на Английский: EN\n
        - с Английского: EN на Русский: RU


        \nПоприветствуй(те) меня, напиши(те): \n\nпривет \n\n - а иначе.....
         \n\nтебя(вас) ждет тренировка на 'котиках' \n или
        \nПЕРЕВОД ПЛАТНЫЙ. НЕ ЗАПЛАТИШЬ, НЕ  БУДЕТ ПЕРЕВОДА)).\
        """)


# обработчик текстовых сообщений
@bot.message_handler(content_types=['text'])
def send_languages(message):
    """ Respond to text messages. """
    if message.text.lower() == "привет":
        chat_id = message.chat.id
        # создаем кнопки для выбора языка перевода
        lang_buttons = types.InlineKeyboardMarkup(row_width=2)
        # lang_buttons.row_width = 2
        lang_buttons1 = types.InlineKeyboardButton("RU-EN", callback_data="ru-en")
        lang_buttons2 = types.InlineKeyboardButton("EN-RU", callback_data="en-ru")
        lang_buttons.add(lang_buttons1, lang_buttons2)
        bot.send_message(chat_id, "Выберите варианты для перевода:", reply_markup=lang_buttons)

    else:
        # отправляем сообщение с кнопками
        bot.send_message(message.from_user.id, "Я тебя(вас) не понимаю. \n\nБУДЬ(ТЕ) ВЕЖЛИВЫМ(И). \nА лучше тогда: сперва потренируйся на 'котиках'. \n\nА теперь. Начни(те) заново. \n\nНапиши(те) или нажмите на  /start.")


# обработчик для кнопок (выбор языка перевода)
@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    try:
        if call.data == "ru-en":
            selectLang['language'] = "RU-EN"
            bot.send_message(call.from_user.id, 'Введи(те) текст на русском языке, который нужно перевести:')
        elif call.data == "en-ru":
            selectLang['language'] = 'EN-RU'
            bot.send_message(call.from_user.id, 'Enter the text in English that you want to translate:')
        # вызываем функцию translate_text, передавая ей текст для перевода
        bot.register_next_step_handler(call.message, translate_text)
    except:
        bot.send_message(call.from_user.id, 'Что-то пошло не так повторите попытку позже')


# функция-переводчик
def translate_text(message):
    """ Translate text using Google Translate API. """
    chat_id = message.chat.id

    if selectLang['language'] == 'RU-EN':
        try:
            translated_text = translator.translate(message.text, dest='en').text
            bot.send_message(chat_id, f'Перевод с русского на английский:\n\n {translated_text}')
            selectLang.pop('language')

            # отправка сообщения с благодарностью за использование бота
            bot.send_message(chat_id, 'Спасибо за использование бота-переводчика!\n\n Если хотите начать заново, нажмите /start')
        except:
            bot.send_message(chat_id, 'Не удалось перевести текст, попробуйте еще раз')

    elif selectLang['language'] == 'EN-RU':
        try:
            translated_text = translator.translate(message.text, dest='ru').text
            bot.send_message(chat_id, f'Перевод с английского на русский:\n\n {translated_text}')
            selectLang.pop('language')

            # отправка сообщения с благодарностью за использование бота
            bot.send_message(chat_id, 'Спасибо за использование бота-переводчика!\n\n Если хотите начать заново, нажмите /start')
        except:
            bot.send_message(chat_id, 'Не удалось перевести текст, попробуйте еще раз')

    else:
        bot.send_message(chat_id, 'Я не понимаю, что вы хотите сделать. Пожалуйста, начните с /start.')


bot.polling()

# bot.infinity_polling(non_stop=True, interval=0)


# token ="6259230595:AAFEZo0v_V6U4VBOsS5yylH22e219WgvEfM"