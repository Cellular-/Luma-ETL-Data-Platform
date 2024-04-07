import json

def get():
    subject_areas = None
    with open('resources/subject_area_configuration_mappings.json', 'r') as sa:
        subject_areas = json.load(sa)

    active_sa =     ["billing",
    "budget",
    "cash",
    "custodial",
    "payables",
    "project",
    "purchasing",
    "receivables",
    "general_ledger",
    "human_resources",
    "workforce_management",
    "maintenance",
    "xm"]

    with open('resources/table_configuration_mappings.json', 'r') as f:
        bc_metadata = json.load(f)
        for sa in active_sa:
            for bc in subject_areas[sa]:
                print(bc_metadata[bc]['business_class_name'])
            
if __name__ == '__main__':
    get()