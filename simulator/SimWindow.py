from tkinter import *
import tkinter.messagebox as tmb
from decimal import Decimal


class Window:
    def __init__(self):
        """Loads the Graphical User Interface. """

        self.root = Tk()
        self.root.wm_title("Stock simulator v1.0")
        self.root.minsize(width=500, height=500)
        self.root.protocol("WM_DELETE_WINDOW", self.askQuit)
        self.stockPrice = DoubleVar()
        self.currentBal = DoubleVar()
        self.portVal = DoubleVar()
        self.error = StringVar()
        self.currentBal.set("{:.2f}".format(self.controller.
                                            getCurrentBalance()))
        self.displayLabel = Label(self.root, text="Enter a ticker: ")
        self.searchBar = Entry(self.root)
        self.searchButton = Button(self.root, text="Go", command=lambda:
        self.checkData())

        self.buyButton = Button(self.root, text="Buy", state=DISABLED,
                                command=lambda: self.buyStock())
        self.sellButton = Button(self.root, text="Sell", state=DISABLED,
                                 command=lambda: self.sellStock())
        self.stockAmountBar = Entry(self.root)
        self.balanceLabel = Label(self.root, text="Current balance: ")
        self.balanceAmount = Label(self.root, textvariable=self.currentBal)
        self.tickerDisplay = Label(self.root, anchor=E, text="Stock price:")
        self.tickerPrice = Label(self.root, textvariable=self.stockPrice)
        self.errorLabel = Label(self.root, textvariable=self.error)
        self.viewPortfolio = Button(self.root, text="View Portfolio",
                                    command=lambda: self.getPortfolio())
        self.portfolioLabel = Label(self.root, text="Portfolio Value:")
        self.portfolioValue = Label(self.root, textvariable=self.portVal)
        self.displayLabel.grid(row=0, column=0, sticky=E)
        self.searchBar.grid(row=0, column=1)
        self.searchButton.grid(row=0, column=2, sticky=W)
        self.balanceLabel.grid(row=1, column=0, sticky=E)
        self.balanceAmount.grid(row=1, column=1, sticky=W)
        self.tickerDisplay.grid(row=2, column=0, sticky=E)
        self.tickerPrice.grid(row=2, column=1, sticky=W)
        self.stockAmountBar.grid(row=0, column=3)
        self.buyButton.grid(row=0, column=4)
        self.sellButton.grid(row=1, column=4, sticky=E)
        self.errorLabel.grid(row=1, columnspan=3, sticky=E)
        self.portfolioLabel.grid(row=4, column=0)
        self.portfolioValue.grid(row=4, column=1, sticky=W)
        self.viewPortfolio.grid(row=4, column=2)
        self.portVal.set(self.controller.getPortfolioValue())
        self.root.mainloop()

