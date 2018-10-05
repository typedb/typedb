/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

import XHRmock from 'xhr-mock';

class LocalStorageMock {
  constructor() {
    this.store = {};
  }

  clear() {
    this.store = {};
  }

  getItem(key) {
    return this.store[key];
  }

  setItem(key, value) {
    this.store[key] = value.toString();
  }
}

export const MockLocalStorage = () => {
  global.localStorage = new LocalStorageMock();
};

export const MockXHR = () => {
  XHRmock.setup();
};

export const stopXHRMock = () => { XHRmock.teardown(); };

 // Jest uses jsdom to mock HTML/DOM elements, but jsdom doesn't ship with:
 // - requestAnimationFrame, so a polyfill is needed.
 // - support for canvas elements, so canvas-prebuilt package is used.

export const requestAnimationFramePolyFill = () => {
  const vendors = ['ms', 'moz', 'webkit', 'o'];
  const af = 'AnimationFrame';
  let lastTime = 0;

  if ('performance' in window === false) { window.performance = {}; }

  if (!Date.now) { Date.now = () => new Date().getTime(); }

  if ('now' in window.performance == false) {
    let nowOffset = Date.now();

    if (performance.timing && performance.timing.navigationStart) { nowOffset = performance.timing.navigationStart; }

    window.performance.now = () => Date.now() - nowOffset;
  }

  for (let x = 0; x < vendors.length && !window.requestAnimationFrame; ++x) {
    const vendor = vendors[x];
    window.requestAnimationFrame = window[`${vendor}Request${af}`];
    window.cancelAnimationFrame = window[`${vendor}Cancel${af}`] || window[`${vendor}CancelRequest${af}`];
  }

  if (!window.requestAnimationFrame) {
    window.requestAnimationFrame = (callback) => {
      const currTime = Date.now();
      const timeToCall = Math.max(0, 16 - (currTime - lastTime));
      const id = window.setTimeout(() => callback(currTime + timeToCall), timeToCall);

      lastTime = currTime + timeToCall;
      return id;
    };
  }

  if (!window.cancelAnimationFrame) { window.cancelAnimationFrame = id => clearTimeout(id); }
};
