const express = require("express");
const bodyParser = require("body-parser");
const fs = require("fs");
const spawn = require("child_process").spawn;
const app = express();
const execSync = require("child_process").execSync;

app.use(bodyParser.json());

app.get("/lengthCounts", async (req, res) => {
  try {
    const data = fs.readFileSync("ouput/part-00000", "utf8");
    // console.log(data);
    const lines = data.split("\n");
    const map = new Map();
    for (const line of lines) {
      if (line.length === 0) {
        continue;
      }
      const [key, val] = line.replace(/\s+/g, "").match(/\d+/g);
      map.set(parseInt(key), parseInt(val));
    }
    // console.log(map);
    const result = Object.fromEntries(map);
    return res.json(result);
  } catch (err) {
    console.error(err);
    return res.sendStatus(500);
  }
});

app.post("/analyze", async (req, res) => {
  try {
    // console.log(req.body);
    try {
      const result = execSync("rm -r output4/");
      console.log(result.toString("utf8"));
    } catch (e) {
      console.log(e);
    }

    const wordlist = req.body.wordlist;
    const weights = req.body.weights;
    const weightKeys = Object.values(weights);

    const pythonProcess = spawn("python3", [
      "spark4.py",
      wordlist.join(","),
      weightKeys.join(","),
    ]);
    console.log(wordlist.join(","));
    console.log(weightKeys.join(","));
    return res.sendStatus(200);
  } catch (err) {
    console.error(err);
    return res.sendStatus(500);
  }
});

app.get("/result", async (req, res) => {
  try {
    if (!fs.existsSync("output4/part-00000")) {
      return res.send("Not done yet");
    }
    const data = fs.readFileSync("output4/part-00000", "utf8");
    console.log(data);
    const lines = data.split("\n");
    const map = new Map();
    for (const line of lines) {
      if (line.length === 0) {
        continue;
      }
      const commaIdx = line.indexOf(",");
      const key = line.substring(2, commaIdx - 1);
      const val = line.substring(commaIdx + 3, line.length - 2);
      map.set(key, val);
    }
    console.log(map);
    const result = Object.fromEntries(map);
    return res.json(result);
  } catch (err) {
    console.error(err);
    return res.sendStatus(500);
  }
});

let http = require("http").Server(app);
const PORT = 80;

http.listen(PORT, function () {
  console.log("start listen");
});
