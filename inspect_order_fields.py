import comtypes.client
comtypes.client.GetModule(r'SKCOM.dll')
import comtypes.gen.SKCOMLib as sk

# Try to introspect FUTUREPROXYORDER
try:
    order = sk.FUTUREPROXYORDER()
    print("FUTUREPROXYORDER available attributes:")
    for attr in dir(order):
        if not attr.startswith('_'):
            print(f"  {attr}")
except Exception as e:
    print(f"Error: {e}")
