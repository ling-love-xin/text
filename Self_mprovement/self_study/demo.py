# -*- coding: utf-8 -*-
# @Time : 2019/12/13 10:50
# @Author : ling
# @Email : ysling129@126.com
# @File : demo
# @Project : Self_mprovement
# @description :
# """
#  模拟3D星空-海龟画图版-星星从右边出来.
#
# """
# from turtle import *
# from random import random,randint
#
# screen = Screen()
# width ,height = 800,600
# screen.setup(width,height)
# screen.title("模拟3D星空_海龟画图版_作者:李兴球")
# screen.bgcolor("black")
# screen.mode("logo")
# screen.delay(0) # 这里要设为0，否则很卡
#
# t = Turtle(visible = False,shape='circle')
# t.pencolor("white")
# t.fillcolor("white")
# t.penup()
# t.setheading(-90)
# t.goto(width/2,randint(-height/2,height/2))
#
# stars = []
# for i in range(200):
#     star = t.clone()
#     s =random() /3
#     star.shapesize(s,s)
#     star.speed(int(s*10))
#     star.setx(width/2 + randint(1,width))
#     star.sety( randint(-height/2,height/2))
#     star.showturtle()
#     stars.append(star)
#
# while True:
#     for star in stars:
#         star.setx(star.xcor() - 3 * star.speed())
#         if star.xcor()<-width/2:
#             star.hideturtle()
#             star.setx(width/2 + randint(1,width))
#             star.sety( randint(-height/2,height/2))
#             star.showturtle()
import turtle
t = turtle.Pen()
turtle.bgcolor("black")
sides=6
colors=["red","yellow","green","blue","orange","purple"]
for x in range(360):
    t.pencolor(colors[x%sides])
    t.forward(x*3/sides+x)
    t.left(360/sides+1)
    t.width(x*sides/200)


