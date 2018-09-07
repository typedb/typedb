<template>
<div>
    <nav role="navigation" class="navbar-fixed noselect">

        <div class="nav-wrapper">
            <span>Grakn Workbase - Visualiser</span>
        </div>
    </nav>
</div>
</template>

<style scoped>

.nav-wrapper {
    display: flex;
    padding-top: 7px;
    justify-content: center;
    align-items: center;
}

.navbar-fixed {
    position: relative;
    z-index: 10;
    min-height: 25px;

    /* Make navbar draggable area in app.*/
    -webkit-app-region:drag;
    border-bottom: var(--container-darkest-border);
}

.dark .navbar-fixed{
    background-color: var(--gray-1);
}

.light .navbar-fixed{
    background-color:lightblue;
}

</style>

<script>

export default {
};
</script>
