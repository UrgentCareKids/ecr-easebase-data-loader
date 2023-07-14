import sys

def hello2(word):
    print('Hello', word)

if __name__ == '__main__':
    word = sys.argv[1]
    hello2(word)
