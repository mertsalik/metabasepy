from pydoc import cli
from metabasepy import Client, RequestException
from pprint import pprint
import secrets
import string
from os.path import exists

if not exists('password.txt'):
  alphabet = string.ascii_letters + string.digits
  password = ''.join(secrets.choice(alphabet) for i in range(10))
  with open("password.txt", "w") as f:
    f.write(password)

client = Client(username='i.sapunov@epoch8.co', password="A2JePEbgTjFFDk", base_url="https://metabase.cw.scaliolabs.com")
# client.setup(path_to_cred_file="collab.json")
client.authenticate()


fields = client.tables.fields(587)
names = {field['name']:field['id'] for field in fields}
print(names)
# table_name = client.fields.get_by_name_and_table("investor_account_id", 587)
# print(table_name)
# table_id = client.tables.get_by_name_and_schema(table_name["name"], table_name["table"]["schema"])
# print(table_id)
# users_dashboard = client.dashboards.get(1)

# users_dashboard_cards = client.cards.get()
# cards = [card['name'] for card in users_dashboard_cards]
# print(users_dashboard_cards)
# print(users_dashboard_cards[1]['card'])