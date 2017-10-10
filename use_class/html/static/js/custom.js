function changeColor(ele){
    ele.style.backgroundColor = "brown";

}

function verifyUser(){

    var ele = document.getElementsByName("username")[0];
    if (ele.value.length == 0) {
        ele.value = "不能输入空";
    }
}
