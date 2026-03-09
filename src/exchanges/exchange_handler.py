#TODO: Move all exchange interaction to here 
# 
# # # exchange_handler.py
# import time
# from typing import Dict, List, Any, Tuple

# class HyperliquidHandler:
#     def __init__(self, exchange_client, logger):
#         self.ex = exchange_client
#         self.logger = logger

#         self.MAX_PRECISION = 6

#     def get_execution_plan(
#         order_intentions: dict, 
#         open_orders: list, 
#         ltps: dict, 
#         sz_decimals: dict, 
#         logger: object,
#         slippage_bps: int = 1  # 1 basis point = 0.01%
#     ) -> tuple[list, list]:
        
#         exchange_orders = []
#         cancel_requests = []
#         MAX_PRECISION = 6 

#         for coin, intention in order_intentions.items():
#             delta = intention["delta"]
#             target_qty = intention["target"]
#             current_qty = intention["current"]
#             side = intention["side"]
            
#             ltp = ltps.get(coin, 0)
#             if ltp == 0:
#                 logger.error(f"Missing LTP data for {coin}")
#                 continue

#             # 1. Dollar Value Logic ($10 Min)
#             dollar_target = abs(target_qty) * ltp
#             dollar_delta = abs(delta) * ltp

#             if dollar_target < 10:
#                 if current_qty != 0: # Force close position
#                     target_qty = 0
#                     delta = -current_qty
#                     dollar_delta = abs(delta) * ltp
#                 else:
#                     continue

#             if round(dollar_delta, 2) < 10 and target_qty != 0:
#                 continue

#             # 2. Price with Slippage Logic
#             # Apply slippage: buy higher, sell lower to ensure fill
#             slippage_factor = (1 + (slippage_bps / 10000)) if side.upper() == "BUY" else (1 - (slippage_bps / 10000))
#             adj_px = ltp * slippage_factor
            
#             sz_dec = sz_decimals.get(coin, 0)
#             clean_sz = abs(round(delta, sz_dec))
            
#             if clean_sz == 0:
#                 continue

#             # Rounding to HL significant figure standards
#             precision = max(0, MAX_PRECISION - sz_dec)
#             clean_px = float(f"{adj_px:.5g}")
#             clean_px = round(clean_px, precision)

#             # 3. Construct Order object
#             exchange_orders.append({
#                 "coin": coin,
#                 "is_buy": side.upper() == "BUY",
#                 "sz": clean_sz,
#                 "limit_px": clean_px,
#                 "order_type": {"limit": {"tif": "Gtc"}},
#                 "reduce_only": False
#             })

#             # 4. Identify Stale Orders
#             open_order = next((o for o in open_orders if o['coin'] == coin), None)
#             if open_order:
#                 cancel_requests.append({"coin": coin, "oid": open_order["oid"]})
#         return exchange_orders, cancel_requests

#     def execute_batch(self, orders, cancels, run_id, wallet_address, order_logger):
#         """Orchestrates bulk cancels and placements with detailed logging."""
        
#         # 1. Handle Bulk Cancels
#         if cancels:
#             self.logger.info(f"Cancel Requests: {cancels}")
#             try:
#                 # Standard HL SDK format for bulk_cancel is a list of dicts: 
#                 # [{"coin": "BTC", "oid": 123}, ...]
#                 cancel_resp = self.ex.bulk_cancel(cancels)
#                 self.logger.info(f"Cancel Response: {cancel_resp}")
#             except Exception as e:
#                 self.logger.error(f"Critical failure during bulk cancel: {e}")

#         # 2. Handle Bulk Placements
#         if not orders:
#             self.logger.info("No new orders to place.")
#             return

#         try:
#             response = self.ex.bulk_orders(orders)
            
#             if response.get("status") == "ok":
#                 all_statuses = response["response"]["data"]["statuses"]
                
#                 for i, status in enumerate(all_statuses):
#                     order_info = orders[i]
                    
#                     # Wrap single status for your existing logger
#                     individual_mock = {
#                         "response": {
#                             "data": {
#                                 "statuses": [status]
#                             }
#                         }
#                     }

#                     order_logger.log_order_submission(
#                         run_id=run_id,
#                         exchange="hyperliquid",
#                         account=wallet_address,
#                         symbol=order_info["coin"],
#                         side="buy" if order_info["is_buy"] else "sell",
#                         order_type="LIMIT",
#                         price=order_info["limit_px"],
#                         qty=order_info["sz"],
#                         response=individual_mock
#                     )
#             else:
#                 self.logger.error(f"Bulk placement failed: {response}")
                
#         except Exception as e:
#             self.logger.error(f"Critical failure during bulk placement: {e}")