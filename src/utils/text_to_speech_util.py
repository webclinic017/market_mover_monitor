import pyttsx3

def get_text_to_speech_engine():
    text_to_speech_engine = pyttsx3.init()

    text_to_speech_engine.setProperty('voice', text_to_speech_engine.getProperty('voices')[1].id)
    text_to_speech_engine.setProperty('rate', 160)
    text_to_speech_engine.setProperty('volume', 1)

    return text_to_speech_engine