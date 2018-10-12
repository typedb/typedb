<template>
    <label class="switch"><input type="checkbox" :checked="isToggled" @change="toggled"><span class="slider round"></span></label>
</template>

<script>
  export default {
    name: 'VueSwitch',
    props: ['isToggled'],
    methods: {
      toggled(val) {
        this.$emit('toggled', val.currentTarget.checked);
      },
    },
  };
</script>

<style scoped>
    /* The switch - the box around the slider */
    .switch {
        position: relative;
        display: inline-block;
        width: 40px;
        height: 18px;
    }

    /* Hide default HTML checkbox */
    .switch input {display:none;}

    /* The slider */
    .slider {
        position: absolute;
        cursor: pointer;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background-color: #ccc;
        -webkit-transition: .4s;
        transition: .4s;
    }

    .slider:before {
        position: absolute;
        content: "";
        height: 12px;
        width: 12px;
        left: 4px;
        bottom: 2px;
        background-color: var(--gray-5);
        -webkit-transition: .4s;
        transition: .4s;
    }

    input:checked + .slider {
        background-color: var(--green-4);
    }

    input:focus + .slider {
        box-shadow: 0 0 1px #2196F3;
    }

    input:checked + .slider:before {
        -webkit-transform: translateX(18px);
        -ms-transform: translateX(18px);
        transform: translateX(18px);
    }

    /* Rounded sliders */
    .slider.round {
        border-radius: 50px;
        border: var(--container-darkest-border);
        background-color: var(--gray-1);

    }

    .slider.round:before {
        border-radius: 50%;
    }


</style>
