from pydoc import cli
from metabasepy import Client, RequestException
from pprint import pprint

client = Client(username='i.sapunov@epoch8.co', password="A2JePEbgTjFFDk", base_url="https://metabase.cw.scaliolabs.com")
client.authenticate()
items = client.collections.items("root")
# cards = client.cards.get()
# for card in cards:
#     card["collection_id"] = 9

#     create = client.cards.post(**card)

dashboards = client.dashboards.get(1)

dashboards["collection_id"] = 9
create = client.dashboards.post(**dashboards)
# pprint(dashboards)

def copy_dashboard(self, source_dashboard_name=None, source_dashboard_id=None, 
                    source_collection_name=None, source_collection_id=None,
                    destination_dashboard_name=None, 
                    destination_collection_name=None, destination_collection_id=None,
                    deepcopy=False, postfix=''):
    """
    Copy the dashboard with the given name/id to the given destination collection. 
    
    Keyword arguments:
    source_dashboard_name -- name of the dashboard to copy (default None) 
    source_dashboard_id -- id of the dashboard to copy (default None) 
    source_collection_name -- name of the collection the source dashboard is located in (default None) 
    source_collection_id -- id of the collection the source dashboard is located in (default None) 
    destination_dashboard_name -- name used for the dashboard in destination (default None).
                                  If None, it will use the name of the source dashboard + postfix.
    destination_collection_name -- name of the collection to copy the dashboard to (default None) 
    destination_collection_id -- id of the collection to copy the dashboard to (default None) 
    deepcopy -- whether to duplicate the cards inside the dashboard (default False).
                If True, puts the duplicated cards in a collection called "[dashboard_name]'s duplicated cards" 
                in the same path as the duplicated dashboard.
    postfix -- if destination_dashboard_name is None, adds this string to the end of source_dashboard_name 
               to make destination_dashboard_name
    """
    ### Making sure we have the data that we need 
    if not source_dashboard_id:
      if not source_dashboard_name:
        raise ValueError('Either the name or id of the source dashboard must be provided.')
      else:
        source_dashboard_id = self.get_item_id(item_type='dashboard',item_name=source_dashboard_name, 
                                               collection_id=source_collection_id, 
                                               collection_name=source_collection_name)
    
    if not destination_collection_id:
      if not destination_collection_name:
        raise ValueError('Either the name or id of the destination collection must be provided.')
      else:
        destination_collection_id = self.get_collection_id(destination_collection_name)
    
    if not destination_dashboard_name:
      if not source_dashboard_name:
        source_dashboard_name = self.get_item_name(item_type='dashboard', item_id=source_dashboard_id)
      destination_dashboard_name = source_dashboard_name + postfix

    ### shallow-copy
    shallow_copy_json = {'collection_id':destination_collection_id, 'name':destination_dashboard_name}
    res = self.post('/api/dashboard/{}/copy'.format(source_dashboard_id), json=shallow_copy_json)
    dup_dashboard_id = res['id']
    
    ### deepcopy
    if deepcopy:
      # Getting the source dashboard info
      source_dashboard = self.get('/api/dashboard/{}'.format(source_dashboard_id))
      
      # creating an empty collection to copy the cards into it
      res = self.post('/api/collection/', 
                      json={'name':destination_dashboard_name + "'s cards", 
                            'color':'#509EE3', 
                            'parent_id':destination_collection_id})
      cards_collection_id = res['id']

      # duplicating cards and putting them in the created collection and making a card_id mapping
      source_dashboard_card_IDs = [ i['card_id'] for i in source_dashboard['ordered_cards'] if i['card_id'] is not None ]
      card_id_mapping = {}
      for card_id in source_dashboard_card_IDs:
        dup_card_id = self.copy_card(source_card_id=card_id, destination_collection_id=cards_collection_id)
        card_id_mapping[card_id] = dup_card_id

      # replacing cards in the duplicated dashboard with duplicated cards
      dup_dashboard = self.get('/api/dashboard/{}'.format(dup_dashboard_id))
      for card in dup_dashboard['ordered_cards']:
        
        # ignoring text boxes. These get copied in the shallow-copy stage.
        if card['card_id'] is None:
          continue
          
        # preparing a json to be used for replacing the cards in the duplicated dashboard
        new_card_id = card_id_mapping[card['card_id']]
        card_json = {}
        card_json['cardId'] = new_card_id
        for prop in ['visualization_settings', 'col', 'row', 'sizeX', 'sizeY', 'series', 'parameter_mappings']:
          card_json[prop] = card[prop]
        for item in card_json['parameter_mappings']:
          item['card_id'] = new_card_id
        # removing the card from the duplicated dashboard
        dash_card_id = card['id'] # This is id of the card in the dashboard (different from id of the card itself)
        self.delete('/api/dashboard/{}/cards'.format(dup_dashboard_id), params={'dashcardId':dash_card_id})
        # adding the new card to the duplicated dashboard
        self.post('/api/dashboard/{}/cards'.format(dup_dashboard_id), json=card_json)
      
    return dup_dashboard_id