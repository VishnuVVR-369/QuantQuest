const FyersAPI = require("fyers-api-v3").fyersModel
var fyers = new FyersAPI();
fyers.setAppId(process.env.FYERS_APP_ID);

fyers.setRedirectUrl(`https://127.0.0.1:5000/`);
var generateAuthcodeURL = fyers.generateAuthCode();

console.log(generateAuthcodeURL)