import pymongo

current_collection_name = 'Accounts'
collection_name = 'accounts'


def run_indexes_migration(
    collection_name: str,
    current_collection_name: str,
):
    environment = 'dev'

    current_host = 'some_shard_1'

    configuration = {
        'dev': {
            'username': '',
            'password': '',
            'host': '',
        },
        'prod': {
            'username': '',
            'password': '',
            'host': '',
        },
    }

    self_hosted_client = pymongo.MongoClient(
        host=current_host,
        port=27017,
    )

    atlas_client = pymongo.MongoClient(
        host=configuration[environment]['host'],
        username=configuration[environment]['username'],
        password=configuration[environment]['password'],
    )

    self_hosted_client_database = self_hosted_client['frontend_database']

    atlas_client_database = atlas_client['frontend_database']

    current_indexes = self_hosted_client_database[current_collection_name].list_indexes()

    existing_collections = atlas_client_database.list_collection_names()

    if collection_name not in existing_collections:
        atlas_client_database.create_collection(
            name=collection_name,
        )

    for current_index in current_indexes:
        if current_index['name'] == '_id_':
            continue

        new_name = current_index['name'].replace('zone', 'location').replace('Zone', 'location')

        new_keys = []

        for index_key, value in current_index['key'].items():
            new_keys.append(
                (
                    index_key.replace('zone', 'location').replace('Zone', 'location'),
                    int(value),
                ),
            )

        unique = current_index.get('unique', False)

        if unique:
            new_name = new_name.replace('location_1_', '').replace('location', '')

            new_name = f'location_1_id_1_{new_name}'

            sharded_keys = [
                (
                    'location',
                    1
                ),
                (
                    '_id',
                    1,
                ),
            ]

            if (
                'location',
                1,
            ) in new_keys:
                new_keys.remove(
                    (
                        'location',
                        1,
                    )
                )

            sharded_keys.extend(new_keys)

            new_keys = sharded_keys

        atlas_client_database[collection_name].create_index(
            keys=new_keys,
            name=new_name,
            unique=unique,
        )


run_indexes_migration(
    collection_name=collection_name,
    current_collection_name=current_collection_name,
)
