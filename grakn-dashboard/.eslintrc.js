module.exports = {
    "extends": "airbnb-base",
    "plugins": [
        "import",
        "html"
    ],
    "globals": {
        "window": true,
        "localStorage": true,
        "$": true,
        "visualiser": true,
        "vis":true,
        "XMLHttpRequest":true,
    },
    "rules": {
        "no-plusplus": [
            1,
            {
                "allowForLoopAfterthoughts": true
            }
        ],
        "max-len": [
            2,
            180
        ],
        "no-unused-vars": [
            2,
            {
                "args": "none"
            }
        ]
    }
};
