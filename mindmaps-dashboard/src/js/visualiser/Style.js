/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

"use strict";

import * as API from '../HAL/APITerms';

/*
 * Styling options for visualised graph.
 */
export default class Style {
    constructor() {
        this.network = {
            background: "#383838"
        };

        this.node = {
            colour: {
                background: "#383838",
                border: "#a1d884",
                highlight: {
                    background: "#383838",
                    border: "#a1d884"
                }
            },
            shape: "box"
        };

        this.edge = {
            colour: {
                color: "#bbbcbc",
                highlight: "#a1d884"
            },
            font: {
                color: "#bbbcbc",
                background: "none",
                strokeWidth: 0
            }
        };
    }

    /**
     * Return node colour based on its @baseType or default colour otherwise.
     * @param baseType
     * @returns {*}
     */
    getNodeColour(baseType) {
        switch(baseType) {
            case API.RELATION_TYPE:
                return {
                    background: this.node.colour.background,
                    border: "#77dd77",
                    highlight: {
                        border: "#77dd77"
                    }
                };
            case API.TYPE_TYPE:
                return {
                    background: this.node.colour.background,
                    border: "#5bc2e7",
                    highlight: {
                        border: "#5bc2e7"
                    }
                };
            case API.RESOURCE_TYPE:
                return {
                    background: this.node.colour.background,
                    border: "#ff7878",
                    highlight: {
                        border: "#ff7878"
                    }
                };
            default:
                return this.node.colour;
        }
    }

    /**
     * Return node shape configuration based on its @baseType or default shape otherwise.
     * @param baseType
     * @returns {string}
     */
    getNodeShape(baseType) {
        switch(baseType) {
            case "resource-type":
            case "relation-type":
            case "entity-type":
            default:
                return this.node.shape;
        }
    }

    /**
     * Return node label font configuration based on its @baseType or default font otherwise.
     * @param baseType
     * @returns {{color: (string|string|string)}}
     */
    getNodeFont(baseType) {
        return {
            color: this.getNodeColour(baseType).border
        };
    }

    /**
     * Return edge colour configuration.
     * @returns {ENGINE.Style.edge.colour|{color, highlight}}
     */
    getEdgeColour() {
        return this.edge.colour;
    }

    /**
     * Return edge label font configuration.
     * @returns {{color: string}}
     */
    getEdgeFont() {
        return this.edge.font;
    }
};
