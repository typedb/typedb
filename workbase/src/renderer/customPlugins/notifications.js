import Vue from 'vue';
import Toasted from 'vue-toasted';
import logger from '@/../Logger';

Vue.use(Toasted);

const SUCCESS_DURATION = 4000;
const INFO_DURATION = 5000;

function registerNotifications() {
  Vue.prototype.$notifySuccess = function successFn(message) {
    this.$toasted.clear();
    this.$toasted.show(message, {
      action: {
        text: 'CLOSE',
        onClick: (e, toastObject) => {
          toastObject.goAway(0);
        },
      },
      duration: SUCCESS_DURATION,
      position: 'bottom-right',
    });
  };

  Vue.prototype.$notifyInfo = function infoFn(message) {
    this.$toasted.show(message, {
      action: {
        text: 'CLOSE',
        onClick: (e, toastObject) => {
          toastObject.goAway(0);
        },
      },
      duration: INFO_DURATION,
      position: 'bottom-right',
      type: 'info',
    });
  };

  Vue.prototype.$notifyConfirmDelete = function confirmFn(message, confirmCb) {
    this.$toasted.clear();
    this.$toasted.show(message, {
      action: [
        {
          text: 'Cancel',
          onClick: (e, toastObject) => {
            toastObject.goAway(0);
          },
        },
        {
          text: 'Confirm',
          onClick: (e, toastObject) => {
            confirmCb();
            toastObject.goAway(0);
          },
          class: 'confirm',
        },
      ],
      position: 'top-center',
    });
  };

  Vue.prototype.$notifyError = function errorFn(e, operation) {
    let errorMessage;
    if (e instanceof Object) {
      if ('message' in e) errorMessage = e.message;
      if (errorMessage.includes('14 UNAVAILABLE')) {
        errorMessage = 'Grakn is not available. <br> - make sure Grakn is running <br> - check that host and port in Grakn URI are correct';
      }
      if (errorMessage.includes('3 INVALID_ARGUMENT: GraknTxOperationException') && errorMessage.includes('read only')) {
        errorMessage = 'The transaction is read only - insert and delete queries are not supported';
      }
      if ((errorMessage.includes('compute') || errorMessage.includes('aggregate')) && !errorMessage.includes('compute path')) {
        errorMessage = 'Compute and aggregate queries are not supported';
      }
      if ('stack' in e) logger.error(e.stack);
    }

    if (typeof e === 'string') {
      errorMessage = e;
      logger.error(e);
    }

    errorMessage = errorMessage.replace(/(?:\r\n|\r|\n)/g, '<br>');
    if (operation) errorMessage = `${errorMessage}<br><br>Action: [${operation}]`;

    this.$toasted.show(errorMessage, {
      action: {
        text: 'CLOSE',
        onClick: (e, toastObject) => {
          toastObject.goAway(0);
        },
      },
      position: 'bottom-right',
      type: 'error',
      className: 'notify-error',
    });
  };
}


export default { registerNotifications };
