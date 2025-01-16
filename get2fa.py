"""
@Function:             
@Author : ZhangPeiCheng
@Time : 2025/1/16 10:47
"""
import pyotp

key = 'WFB5EXZMQE4MH62TXDOFV3PLSBLNXYZH'
totp = pyotp.TOTP(key)
print(totp.now())