module.exports = {
    "extends": "airbnb-base",
    "plugins": [
        "import"
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
        "no-underscore-dangle": [
            0,
            {
                "allow": ["true"]
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
