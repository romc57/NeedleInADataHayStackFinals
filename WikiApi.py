import requests
import json
import wikipedia

EDITOR_LINK_LIST = [
    'https://en.wikipedia.org/w/index.php?title=Wikipedia:List_of_Wikipedians_by_number_of_edits&action=raw&oldid=1031698119',
    'https://en.wikipedia.org/w/index.php?title=Wikipedia:List_of_Wikipedians_by_number_of_edits&action=raw&oldid=1031698148',
    'https://en.wikipedia.org/w/index.php?title=Wikipedia:List_of_Wikipedians_by_number_of_edits&action=raw&oldid=1031698159',
    'https://en.wikipedia.org/w/index.php?title=Wikipedia:List_of_Wikipedians_by_number_of_edits&action=raw&oldid=1031698209',
]


def extract_editors_list(raw_data):
    editor_list = list()
    data = raw_data.split('{')[1]
    data = data.split('|}')[0]
    data = data.split('! User groups\n')[1]
    data_list = data.split('|-')
    for editor in data_list:
        if not editor or editor == '\n':
            continue
        attr_list = editor.split('\n|')
        editor_list.append({
            'idx': attr_list[1],
            'editor_name': attr_list[2],
            'num_of_edits': attr_list[3],
            'roles': attr_list[4].split('\n')[0]
        })


def execute_request(link):
    res = requests.get(link)
    raw_data = res.text
    return raw_data


def get_editor_list():
    output = {'data': list()}
    for link in EDITOR_LINK_LIST:
        raw_data = execute_request(link)
        editors_list = extract_editors_list(raw_data)
        for editor in editors_list:
            output['data'].append(editor)
    with open('data/editors.json', 'w') as writer:
        writer.write(json.dumps(output))


def get_node_connections(node, level, print_info=False):
    if print_info:
        print('Retrieving for node {} on level {}'.format(node, level))
    if level == 0:
        return None
    output = dict()
    try:
        related_articles = wikipedia.page(node).links
    except Exception as problem:
        if print_info:
            print('Problem on node {} at level {}:\n{}'.format(node, level, problem))
        return None
    for article in related_articles:
        output[article] = get_node_connections(article, level - 1)
    return output


def get_editors():
    res = requests.get('https://en.wikipedia.org/w/index.php?title=Help:Creating_a_bot&action=history')
    data = res.text
    print(data)


if __name__ == '__main__':
    # with open('node_connections_food.json', 'w') as writer:
    #     writer.write(json.dumps({'Food': get_node_connections('Food', )}))
    print(get_node_connections('?78lkjfla;sdkjf', 1))