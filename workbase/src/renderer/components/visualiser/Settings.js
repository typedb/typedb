const networkOptions = {
  autoResize: true,
  physics: {
    barnesHut: {
      springLength: 100,
      springConstant: 0.01,
    },
    minVelocity: 1,
  },
  interaction: {
    hover: true,
    multiselect: true,
  },
  layout: {
    improvedLayout: false,
    randomSeed: 10,
  },
};

export default {
  networkOptions,
};
